"""
Webhook handler for receiving data from external sources.

This module provides webhook handling with:
- Signature verification
- Payload parsing
- Type-safe handlers
- Error handling
"""

import hashlib
import hmac
import logging
from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from vibe_piper.schema_definitions import (
    Array,
    Integer,
    Object,
    String,
    define_schema,
)

# =============================================================================
# Webhook Request
# =============================================================================


@dataclass
class WebhookRequest:
    """
    Wrapper for incoming webhook requests.

    Provides convenient access to request data and metadata.
    """

    body: bytes
    """Raw request body"""

    headers: dict[str, str]
    """Request headers"""

    method: str
    """HTTP method"""

    path: str
    """Request path"""

    query_params: dict[str, list[str]]
    """Query parameters"""

    source_ip: str | None = None
    """Source IP address"""

    @property
    def body_text(self) -> str:
        """Get body as text string."""
        return self.body.decode("utf-8")

    @property
    def body_json(self) -> dict[str, Any]:
        """
        Parse body as JSON.

        Returns:
            Parsed JSON data

        Raises:
            ValueError: If body is not valid JSON
        """
        import json

        try:
            return json.loads(self.body_text)
        except json.JSONDecodeError as e:
            msg = f"Invalid JSON: {e}"
            raise ValueError(msg) from e


# =============================================================================
# Signature Verification
# =============================================================================


class SignatureAlgorithm(Enum):
    """Supported signature algorithms."""

    HMAC_SHA1 = auto()
    HMAC_SHA256 = auto()
    HMAC_SHA512 = auto()


@dataclass
class SignatureVerifier:
    """
    Verifier for webhook signatures.

    Supports common signature algorithms used by webhook providers.
    """

    secret: str
    """Shared secret for signature verification"""

    algorithm: SignatureAlgorithm = SignatureAlgorithm.HMAC_SHA256
    """Signature algorithm to use"""

    header_name: str = "X-Signature"
    """Header containing the signature"""

    signature_prefix: str = "sha1="
    """Signature prefix (e.g., 'sha1=' for GitHub webhooks)"""

    def __post_init__(self) -> None:
        """Validate verifier configuration."""
        if not self.secret:
            msg = "Secret cannot be empty"
            raise ValueError(msg)

        # Set default prefix based on algorithm
        if self.algorithm == SignatureAlgorithm.HMAC_SHA256 and self.signature_prefix == "sha1=":
            self.signature_prefix = "sha256="
        elif self.algorithm == SignatureAlgorithm.HMAC_SHA512 and self.signature_prefix == "sha1=":
            self.signature_prefix = "sha512="

    def verify(self, request: WebhookRequest) -> bool:
        """
        Verify webhook request signature.

        Args:
            request: Webhook request to verify

        Returns:
            True if signature is valid

        Raises:
            ValueError: If signature header is missing
        """
        # Get signature from headers
        signature_header = request.headers.get(self.header_name)

        if not signature_header:
            msg = f"Missing signature header: {self.header_name}"
            raise ValueError(msg)

        # Extract signature value (remove prefix if present)
        signature = signature_header
        if signature_header.startswith(self.signature_prefix):
            signature = signature_header[len(self.signature_prefix) :]

        # Calculate expected signature
        expected_signature = self._calculate_signature(request.body)

        # Compare signatures using constant-time comparison
        return hmac.compare_digest(signature, expected_signature)

    def _calculate_signature(self, data: bytes) -> str:
        """
        Calculate signature for data.

        Args:
            data: Data to sign

        Returns:
            Hex-encoded signature
        """
        if self.algorithm == SignatureAlgorithm.HMAC_SHA1:
            digest = hmac.new(
                self.secret.encode(),
                data,
                hashlib.sha1,
            ).hexdigest()
        elif self.algorithm == SignatureAlgorithm.HMAC_SHA256:
            digest = hmac.new(
                self.secret.encode(),
                data,
                hashlib.sha256,
            ).hexdigest()
        elif self.algorithm == SignatureAlgorithm.HMAC_SHA512:
            digest = hmac.new(
                self.secret.encode(),
                data,
                hashlib.sha512,
            ).hexdigest()
        else:
            msg = f"Unsupported algorithm: {self.algorithm}"
            raise ValueError(msg)

        return digest


# =============================================================================
# Webhook Handler
# =============================================================================


WebhookHandlerFn = Callable[[WebhookRequest], Any]
"""Type alias for webhook handler function."""


class WebhookHandler:
    """
    Webhook handler for receiving and processing webhook requests.

    Provides signature verification and routing to handlers.
    """

    def __init__(
        self,
        verifier: SignatureVerifier | None = None,
        require_signature: bool = True,
    ) -> None:
        """
        Initialize webhook handler.

        Args:
            verifier: Optional signature verifier
            require_signature: If True, reject requests without valid signatures
        """
        self.verifier = verifier
        self.require_signature = require_signature
        self._handlers: dict[str, WebhookHandlerFn] = {}
        self._default_handler: WebhookHandlerFn | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def on(
        self,
        event_type: str,
    ) -> Callable[[WebhookHandlerFn], WebhookHandlerFn]:
        """
        Decorator to register a handler for a specific event type.

        Args:
            event_type: Event type to handle

        Returns:
            Decorator function

        Example:
            ```python
            handler = WebhookHandler()

            @handler.on("user.created")
            async def handle_user_created(request: WebhookRequest):
                data = request.body_json
                print(f"User created: {data['user_id']}")
            ```
        """

        def decorator(func: WebhookHandlerFn) -> WebhookHandlerFn:
            self._handlers[event_type] = func
            return func

        return decorator

    def default(self, func: WebhookHandlerFn) -> WebhookHandlerFn:
        """
        Decorator to register a default handler for unmatched events.

        Args:
            func: Handler function

        Returns:
            Handler function

        Example:
            ```python
            handler = WebhookHandler()

            @handler.default
            async def handle_default(request: WebhookRequest):
                print(f"Unhandled event: {request.headers.get('X-Event-Type')}")
            ```
        """
        self._default_handler = func
        return func

    async def handle(self, request: WebhookRequest, event_type: str | None = None) -> Any:
        """
        Handle webhook request.

        Args:
            request: Webhook request to handle
            event_type: Optional event type (extracted from headers if not provided)

        Returns:
            Handler result

        Raises:
            ValueError: If signature verification fails
            KeyError: If no handler is found for event type
        """
        # Verify signature if configured
        if self.verifier:
            if not self.verifier.verify(request):
                msg = "Invalid webhook signature"
                raise ValueError(msg)
        elif self.require_signature:
            msg = "Signature verification required but not configured"
            raise ValueError(msg)

        # Extract event type from headers if not provided
        if event_type is None:
            event_type = request.headers.get("X-Event-Type") or request.headers.get(
                "X-GitHub-Event", ""
            )

        # Find and call handler
        handler = self._handlers.get(event_type, self._default_handler)

        if handler is None:
            # If no handler found and no default, return None
            self._logger.warning("No handler found for event type: %s", event_type)
            return None

        # Call handler
        return await handler(request) if callable(handler) else handler(request)

    def get_supported_events(self) -> list[str]:
        """
        Get list of supported event types.

        Returns:
            List of event type names
        """
        return list(self._handlers.keys())


# =============================================================================
# Framework Integration Helpers
# =============================================================================


def create_request_from_starlette(request: Any) -> WebhookRequest:
    """
    Create WebhookRequest from Starlette/FastAPI request.

    Args:
        request: Starlette/FastAPI request object

    Returns:
        WebhookRequest instance
    """
    import asyncio

    # Note: This is async, so caller needs to await
    # This is just a helper signature
    return WebhookRequest(
        body=asyncio.run(request.body()),
        headers=dict(request.headers),
        method=request.method,
        path=request.url.path,
        query_params=dict(request.query_params),
        source_ip=request.client.host if request.client else None,
    )


def create_request_from_flask(request: Any) -> WebhookRequest:
    """
    Create WebhookRequest from Flask request.

    Args:
        request: Flask request object

    Returns:
        WebhookRequest instance
    """
    return WebhookRequest(
        body=request.get_data(),
        headers=dict(request.headers),
        method=request.method,
        path=request.path,
        query_params=request.args.lists(),
        source_ip=request.remote_addr,
    )


# =============================================================================
# Common Webhook Schemas
# =============================================================================


# GitHub webhook event schema (simplified)
@define_schema
class GitHubPushEvent:
    """GitHub push webhook event schema."""

    ref: String = String()
    repository: Object = Object()
    pusher: Object = Object()
    commits: Array = Array()


# Stripe webhook event schema (simplified)
@define_schema
class StripeEvent:
    """Stripe webhook event schema."""

    id: String = String()
    type: String = String()
    data: Object = Object()


# Slack webhook event schema (simplified)
@define_schema
class SlackEvent:
    """Slack webhook event schema."""

    token: String = String()
    team_id: String = String()
    api_app_id: String = String()
    event: Object = Object()
    type: String = String()
    event_id: String = String()
    event_time: Integer = Integer()
