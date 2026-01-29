"""
API Integration Module

This module provides a comprehensive suite of API client helpers for data ingestion
from external sources. It supports REST, GraphQL, and webhook integrations with
built-in retry logic, rate limiting, authentication, and response validation.

Components:
- Base APIClient class
- RESTClient for REST APIs
- GraphQLClient for GraphQL APIs
- WebhookHandler for receiving webhooks
- Authentication helpers
- Rate limiting and backoff strategies
"""

from vibe_piper.integration.auth import (
    APIKeyAuth,
    AuthStrategy,
    BasicAuth,
    BearerTokenAuth,
    OAuth2ClientCredentialsAuth,
)
from vibe_piper.integration.base import APIClient, APIError, AuthenticationError, RateLimitError
from vibe_piper.integration.graphql import GraphQLClient, GraphQLResponse
from vibe_piper.integration.pagination import (
    CursorPagination,
    LinkHeaderPagination,
    OffsetPagination,
    PaginationStrategy,
)
from vibe_piper.integration.rest import RESTClient, RESTResponse
from vibe_piper.integration.validation import (
    ResponseValidator,
    ValidationResult,
    validate_and_parse,
    validate_response,
)
from vibe_piper.integration.webhook import WebhookHandler, WebhookRequest

__all__ = [
    # Base
    "APIClient",
    "APIError",
    "AuthenticationError",
    "RateLimitError",
    # REST
    "RESTClient",
    "RESTResponse",
    # GraphQL
    "GraphQLClient",
    "GraphQLResponse",
    # Webhook
    "WebhookHandler",
    "WebhookRequest",
    # Auth
    "AuthStrategy",
    "BearerTokenAuth",
    "APIKeyAuth",
    "BasicAuth",
    "OAuth2ClientCredentialsAuth",
    # Pagination
    "PaginationStrategy",
    "CursorPagination",
    "OffsetPagination",
    "LinkHeaderPagination",
    # Validation
    "ResponseValidator",
    "ValidationResult",
    "validate_response",
    "validate_and_parse",
]
