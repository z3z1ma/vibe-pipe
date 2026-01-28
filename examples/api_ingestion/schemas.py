"""Data schema definitions for the API ingestion example."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class UserResponse:
    """Schema for a single user from the API response."""

    id: int
    name: str
    email: str
    username: str
    phone: str | None
    website: str | None
    company: dict[str, Any] | None
    address: dict[str, Any] | None
    created_at: str | None
    updated_at: str | None

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "UserResponse":
        """Create UserResponse from API response dictionary."""
        return cls(
            id=int(data["id"]),
            name=str(data["name"]),
            email=str(data["email"]),
            username=str(data.get("username", "")),
            phone=data.get("phone"),
            website=data.get("website"),
            company=data.get("company"),
            address=data.get("address"),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )

    def to_database_dict(self) -> dict[str, Any]:
        """Convert to database-friendly dictionary."""
        company_name = None
        if self.company:
            company_name = self.company.get("name")

        city = None
        if self.address:
            city = self.address.get("city")

        return {
            "user_id": self.id,
            "name": self.name,
            "email": self.email,
            "username": self.username,
            "phone": self.phone,
            "website": self.website,
            "company_name": company_name,
            "city": city,
            "created_at": (
                datetime.fromisoformat(self.created_at) if self.created_at else None
            ),
            "updated_at": (
                datetime.fromisoformat(self.updated_at) if self.updated_at else None
            ),
        }


@dataclass
class PaginatedResponse:
    """Schema for paginated API response."""

    data: list[dict[str, Any]]
    page: int
    per_page: int
    total: int
    total_pages: int
    has_next: bool
    has_prev: bool

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "PaginatedResponse":
        """Create PaginatedResponse from API response dictionary."""
        return cls(
            data=list(data.get("data", [])),
            page=int(data.get("page", 1)),
            per_page=int(data.get("per_page", 10)),
            total=int(data.get("total", 0)),
            total_pages=int(data.get("total_pages", 0)),
            has_next=bool(data.get("has_next", False)),
            has_prev=bool(data.get("has_prev", False)),
        )


@dataclass
class QualityReport:
    """Data quality report for the ingestion pipeline."""

    total_records: int
    successful_records: int
    failed_records: int
    validation_errors: list[dict[str, Any]]
    api_calls: int
    pages_fetched: int
    start_time: datetime
    end_time: datetime | None
    rate_limit_hits: int
    retry_attempts: int

    def to_dict(self) -> dict[str, Any]:
        """Convert quality report to dictionary."""
        return {
            "total_records": self.total_records,
            "successful_records": self.successful_records,
            "failed_records": self.failed_records,
            "validation_errors": self.validation_errors,
            "api_calls": self.api_calls,
            "pages_fetched": self.pages_fetched,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": (
                (self.end_time - self.start_time).total_seconds()
                if self.end_time
                else None
            ),
            "rate_limit_hits": self.rate_limit_hits,
            "retry_attempts": self.retry_attempts,
            "success_rate": (
                self.successful_records / self.total_records
                if self.total_records > 0
                else 0
            ),
        }

    def print_summary(self) -> None:
        """Print a human-readable summary of the quality report."""
        print("\n" + "=" * 60)
        print("DATA QUALITY REPORT")
        print("=" * 60)
        print(f"Total Records Processed: {self.total_records}")
        print(f"Successful: {self.successful_records}")
        print(f"Failed: {self.failed_records}")
        print(
            f"Success Rate: {(self.successful_records / self.total_records * 100):.2f}%"
        )
        print(f"\nAPI Calls: {self.api_calls}")
        print(f"Pages Fetched: {self.pages_fetched}")
        print(f"Rate Limit Hits: {self.rate_limit_hits}")
        print(f"Retry Attempts: {self.retry_attempts}")

        if self.validation_errors:
            print(f"\nValidation Errors ({len(self.validation_errors)}):")
            for i, error in enumerate(self.validation_errors[:5], 1):
                print(f"  {i}. {error}")
            if len(self.validation_errors) > 5:
                print(f"  ... and {len(self.validation_errors) - 5} more")

        duration = (
            (self.end_time - self.start_time).total_seconds() if self.end_time else 0
        )
        print(f"\nDuration: {duration:.2f} seconds")
        print("=" * 60)
