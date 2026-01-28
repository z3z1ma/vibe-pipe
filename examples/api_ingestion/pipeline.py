# type: ignore
"""API Ingestion Pipeline."""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from vibe_piper.connectors.postgres import PostgreSQLConfig, PostgreSQLConnector
from vibe_piper.integration.base import RateLimiter, RetryConfig
from vibe_piper.integration.pagination import OffsetPagination, fetch_all_pages
from vibe_piper.integration.rest import RESTClient

from .schemas import QualityReport, UserResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


class APIIngestionPipeline:
    """Pipeline for ingesting data from a REST API into a database."""

    def __init__(
        self,
        api_base_url: str,
        api_key: str | None = None,
        db_config: PostgreSQLConfig | None = None,
        rate_limit_per_second: int = 10,
        max_retries: int = 3,
        page_size: int = 100,
    ) -> None:
        """Initialize the API ingestion pipeline."""
        self.api_base_url = api_base_url
        self.page_size = page_size
        self.db_config = db_config

        self._api_calls = 0
        self._pages_fetched = 0
        self._rate_limit_hits = 0
        self._retry_attempts = 0
        self._validation_errors: list[dict[str, Any]] = []

        self.retry_config = RetryConfig(
            max_attempts=max_retries,
            initial_delay=1.0,
            max_delay=60.0,
            exponential_base=2.0,
            jitter=True,
        )

        self.rate_limiter = RateLimiter(
            max_requests=rate_limit_per_second,
            time_window_seconds=1.0,
        )

        headers = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"

        self.rest_client = RESTClient(
            base_url=api_base_url,
            headers=headers,
            retry_config=self.retry_config,
            rate_limiter=self.rate_limiter,
            timeout=30.0,
        )

        self.db_connector: PostgreSQLConnector | None = None
        if db_config:
            self.db_connector = PostgreSQLConnector(db_config)

    async def initialize(self) -> None:
        """Initialize the pipeline (HTTP client, database connection)."""
        await self.rest_client.initialize()

        if self.db_connector:
            self.db_connector.connect()
            self._create_users_table()

    async def close(self) -> None:
        """Close the pipeline resources."""
        await self.rest_client.close()

        if self.db_connector:
            self.db_connector.disconnect()

    def _create_users_table(self) -> None:
        """Create the users table in the database."""
        if not self.db_connector:
            return

        create_table_sql = """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL UNIQUE,
                username VARCHAR(100),
                phone VARCHAR(50),
                website VARCHAR(255),
                company_name VARCHAR(255),
                city VARCHAR(100),
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
            CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
            CREATE INDEX IF NOT EXISTS idx_users_company ON users(company_name);
        """

        try:
            for statement in create_table_sql.split(";"):
                statement = statement.strip()
                if statement:
                    self.db_connector.execute(statement)
            logger.info("Users table created successfully")
        except Exception as e:
            logger.error("Failed to create users table: %s", e)
            raise

    async def fetch_users(
        self,
        start_page: int = 1,
        max_pages: int | None = None,
        filters: dict[str, Any] | None = None,
    ) -> list[UserResponse]:
        """Fetch all users from the API with pagination."""
        logger.info("Starting to fetch users from API...")

        pagination_strategy = OffsetPagination(
            items_path="data",
            total_path="total",
            offset_param="offset",
            limit_param="limit",
            page_size=self.page_size,
        )

        initial_params = {
            "limit": self.page_size,
            "offset": (start_page - 1) * self.page_size,
        }

        if filters:
            initial_params.update(filters)

        self._api_calls = 0
        self._pages_fetched = 0

        try:
            raw_users = []
            page_count = 0

            async for user_data in fetch_all_pages(
                client=self.rest_client,
                path="/users",
                strategy=pagination_strategy,
                method="GET",
                initial_params=initial_params,
            ):
                raw_users.append(user_data)
                self._api_calls += 1

                if len(raw_users) % self.page_size == 0:
                    self._pages_fetched += 1

                page_count += 1
                if max_pages and page_count >= max_pages:
                    logger.info("Reached maximum page limit: %d", max_pages)
                    break

            users = [UserResponse.from_dict(user) for user in raw_users]

            logger.info(
                "Fetched %d users from %d pages (%d API calls)",
                len(users),
                self._pages_fetched,
                self._api_calls,
            )

            return users

        except Exception as e:
            logger.error("Failed to fetch users: %s", e)
            raise

    def transform_user(self, user: UserResponse) -> dict[str, Any] | None:
        """Transform and validate a user record."""
        try:
            user_dict = user.to_database_dict()

            if not user_dict.get("name") or not user_dict.get("email"):
                error = {
                    "user_id": user.id,
                    "error": "Missing required field (name or email)",
                }
                self._validation_errors.append(error)
                logger.warning("Validation failed for user %d: %s", user.id, error)
                return None

            email = user_dict.get("email", "")
            if "@" not in email or "." not in email:
                error = {
                    "user_id": user.id,
                    "error": f"Invalid email format: {email}",
                }
                self._validation_errors.append(error)
                logger.warning("Validation failed for user %d: %s", user.id, error)
                return None

            user_dict["ingested_at"] = datetime.now(UTC)

            return user_dict

        except Exception as e:
            error = {
                "user_id": user.id,
                "error": f"Transformation error: {str(e)}",
            }
            self._validation_errors.append(error)
            logger.error("Error transforming user %d: %s", user.id, e)
            return None

    def load_users(self, users: list[dict[str, Any]]) -> dict[str, int]:
        """Load transformed users into the database."""
        if not self.db_connector:
            logger.warning("No database connector configured, skipping load")
            return {"successful": 0, "failed": 0}

        logger.info("Loading %d users into database...", len(users))

        successful = 0
        failed = 0

        for user_dict in users:
            try:
                upsert_sql = """
                    INSERT INTO users (
                        user_id, name, email, username, phone, website,
                        company_name, city, created_at, updated_at, ingested_at
                    ) VALUES (
                        %(user_id)s, %(name)s, %(email)s, %(username)s,
                        %(phone)s, %(website)s, %(company_name)s, %(city)s,
                        %(created_at)s, %(updated_at)s, %(ingested_at)s
                    )
                    ON CONFLICT (email) DO UPDATE SET
                        name = EXCLUDED.name,
                        username = EXCLUDED.username,
                        phone = EXCLUDED.phone,
                        website = EXCLUDED.website,
                        company_name = EXCLUDED.company_name,
                        city = EXCLUDED.city,
                        updated_at = EXCLUDED.updated_at,
                        ingested_at = EXCLUDED.ingested_at
                """

                self.db_connector.execute(upsert_sql, user_dict)
                successful += 1

            except Exception as e:
                failed += 1
                error = {
                    "user_id": user_dict.get("user_id"),
                    "error": f"Database error: {str(e)}",
                }
                self._validation_errors.append(error)
                logger.error(
                    "Failed to insert user %d: %s", user_dict.get("user_id"), e
                )

        logger.info("Load complete: %d successful, %d failed", successful, failed)

        return {"successful": successful, "failed": failed}

    async def run(
        self,
        start_page: int = 1,
        max_pages: int | None = None,
        filters: dict[str, Any] | None = None,
        dry_run: bool = False,
    ) -> QualityReport:
        """Run the complete API ingestion pipeline."""
        start_time = datetime.now(UTC)
        logger.info("=" * 60)
        logger.info("Starting API Ingestion Pipeline")
        logger.info("=" * 60)

        try:
            users = await self.fetch_users(start_page, max_pages, filters)

            logger.info("Transforming and validating %d users...", len(users))
            transformed_users = [
                self.transform_user(user)
                for user in users
                if self.transform_user(user) is not None
            ]

            load_results = {"successful": 0, "failed": 0}
            if not dry_run and transformed_users:
                load_results = self.load_users(transformed_users)
            elif dry_run:
                logger.info("DRY RUN: Skipping database insertion")

            end_time = datetime.now(UTC)
            report = QualityReport(
                total_records=len(users),
                successful_records=load_results["successful"],
                failed_records=load_results["failed"],
                validation_errors=self._validation_errors,
                api_calls=self._api_calls,
                pages_fetched=self._pages_fetched,
                start_time=start_time,
                end_time=end_time,
                rate_limit_hits=self._rate_limit_hits,
                retry_attempts=self._retry_attempts,
            )

            logger.info("Pipeline completed successfully")
            return report

        except Exception as e:
            logger.error("Pipeline failed: %s", e)
            raise

        finally:
            logger.info("=" * 60)


async def main() -> None:
    """Main entry point for running the API ingestion pipeline."""
    api_base_url = "https://api.example.com/v1"
    api_key = "your-api-key-here"

    db_config = PostgreSQLConfig(
        host="localhost",
        port=5432,
        database="vibe_piper_demo",
        user="postgres",
        password="postgres",
        pool_size=5,
    )

    pipeline = APIIngestionPipeline(
        api_base_url=api_base_url,
        api_key=api_key,
        db_config=db_config,
        rate_limit_per_second=10,
        max_retries=3,
        page_size=100,
    )

    try:
        await pipeline.initialize()
        report = await pipeline.run(dry_run=False)
        report.print_summary()
    finally:
        await pipeline.close()


if __name__ == "__main__":
    asyncio.run(main())
