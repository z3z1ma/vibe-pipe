"""
Authentication strategies for API clients.

This module provides various authentication methods that can be used with API clients:
- Bearer token authentication
- API key authentication (header or query parameter)
- Basic authentication
- OAuth2 client credentials flow
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass

import httpx

# =============================================================================
# Authentication Strategy Protocol
# =============================================================================


class AuthStrategy(ABC):
    """
    Abstract base class for authentication strategies.

    Each strategy knows how to add authentication credentials to HTTP requests.
    """

    @abstractmethod
    async def apply(self, request: httpx.Request) -> None:
        """
        Apply authentication to the request.

        Args:
            request: The HTTP request to authenticate
        """


# =============================================================================
# Bearer Token Authentication
# =============================================================================


@dataclass
class BearerTokenAuth(AuthStrategy):
    """
    Bearer token authentication (RFC 6750).

    Adds an Authorization header with a bearer token.
    """

    token: str
    """The bearer token"""

    header_name: str = "Authorization"
    """Header name for the token"""

    def __post_init__(self) -> None:
        """Validate token configuration."""
        if not self.token:
            msg = "Token cannot be empty"
            raise ValueError(msg)

    async def apply(self, request: httpx.Request) -> None:
        """
        Apply bearer token to request.

        Args:
            request: The HTTP request to authenticate
        """
        request.headers[self.header_name] = f"Bearer {self.token}"


# =============================================================================
# API Key Authentication
# =============================================================================


@dataclass
class APIKeyAuth(AuthStrategy):
    """
    API key authentication.

    Supports adding API keys via headers or query parameters.
    """

    api_key: str
    """The API key"""

    key_name: str = "X-API-Key"
    """Name of the header or query parameter"""

    location: str = "header"
    """Where to add the key: 'header' or 'query'"""

    def __post_init__(self) -> None:
        """Validate API key configuration."""
        if not self.api_key:
            msg = "API key cannot be empty"
            raise ValueError(msg)
        if self.location not in ("header", "query"):
            msg = "Location must be 'header' or 'query'"
            raise ValueError(msg)

    async def apply(self, request: httpx.Request) -> None:
        """
        Apply API key to request.

        Args:
            request: The HTTP request to authenticate
        """
        if self.location == "header":
            request.headers[self.key_name] = self.api_key
        else:  # query
            # Ensure URL has query parameters
            if "?" not in request.url:
                request.url = f"{request.url}?{self.key_name}={self.api_key}"
            else:
                request.url = f"{request.url}&{self.key_name}={self.api_key}"


# =============================================================================
# Basic Authentication
# =============================================================================


@dataclass
class BasicAuth(AuthStrategy):
    """
    HTTP Basic Authentication (RFC 7617).

    Authenticates with a username and password using HTTP Basic Auth.
    """

    username: str
    """Username for authentication"""

    password: str
    """Password for authentication"""

    def __post_init__(self) -> None:
        """Validate basic auth configuration."""
        if not self.username:
            msg = "Username cannot be empty"
            raise ValueError(msg)

    async def apply(self, request: httpx.Request) -> None:
        """
        Apply basic authentication to request.

        Args:
            request: The HTTP request to authenticate
        """
        import base64

        credentials = f"{self.username}:{self.password}"
        encoded = base64.b64encode(credentials.encode()).decode()
        request.headers["Authorization"] = f"Basic {encoded}"


# =============================================================================
# OAuth2 Client Credentials Authentication
# =============================================================================


@dataclass
class OAuth2ClientCredentialsAuth(AuthStrategy):
    """
    OAuth2 Client Credentials flow authentication.

    Automatically obtains and refreshes access tokens using the client credentials flow.
    Implements token caching and automatic refresh before expiry.
    """

    token_url: str
    """URL to obtain access tokens"""

    client_id: str
    """OAuth2 client ID"""

    client_secret: str
    """OAuth2 client secret"""

    scope: str | None = None
    """Optional OAuth2 scope"""

    _access_token: str | None = None
    """Cached access token"""

    _token_expiry: float | None = None
    """Token expiry timestamp"""

    _refresh_buffer: int = 60  # seconds
    """Refresh token this many seconds before expiry"""

    def __post_init__(self) -> None:
        """Validate OAuth2 configuration."""
        if not self.token_url:
            msg = "Token URL cannot be empty"
            raise ValueError(msg)
        if not self.client_id:
            msg = "Client ID cannot be empty"
            raise ValueError(msg)
        if not self.client_secret:
            msg = "Client secret cannot be empty"
            raise ValueError(msg)

    async def apply(self, request: httpx.Request) -> None:
        """
        Apply OAuth2 access token to request.

        Automatically obtains or refreshes token if needed.

        Args:
            request: The HTTP request to authenticate
        """
        token = await self._get_valid_token()
        request.headers["Authorization"] = f"Bearer {token}"

    async def _get_valid_token(self) -> str:
        """
        Get a valid access token, obtaining a new one if necessary.

        Returns:
            Valid access token
        """
        import time

        # Return cached token if still valid
        if self._access_token and self._token_expiry:
            now = time.time()
            if now < (self._token_expiry - self._refresh_buffer):
                return self._access_token

        # Obtain new token
        return await self._obtain_token()

    async def _obtain_token(self) -> str:
        """
        Obtain a new access token from the OAuth2 server.

        Returns:
            New access token

        Raises:
            AuthenticationError: If token request fails
        """
        import time

        from vibe_piper.integration.base import AuthenticationError

        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        if self.scope:
            data["scope"] = self.scope

        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(
                    self.token_url,
                    data=data,
                    headers={"Accept": "application/json"},
                )
                response.raise_for_status()

                token_data = response.json()

                self._access_token = token_data["access_token"]

                # Calculate expiry time
                expires_in = token_data.get("expires_in", 3600)
                self._token_expiry = time.time() + expires_in

                return self._access_token

            except httpx.HTTPStatusError as e:
                raise AuthenticationError(
                    f"Failed to obtain OAuth2 token: {e}",
                    response_body=e.response.text,
                ) from e
            except (KeyError, ValueError) as e:
                raise AuthenticationError(
                    f"Invalid OAuth2 token response: {e}",
                ) from e


# =============================================================================
# Custom Authentication
# =============================================================================


@dataclass
class CustomHeaderAuth(AuthStrategy):
    """
    Custom header authentication.

    Adds arbitrary headers for authentication.
    """

    headers: dict[str, str]
    """Headers to add for authentication"""

    def __post_init__(self) -> None:
        """Validate custom header configuration."""
        if not self.headers:
            msg = "Headers dictionary cannot be empty"
            raise ValueError(msg)

    async def apply(self, request: httpx.Request) -> None:
        """
        Apply custom headers to request.

        Args:
            request: The HTTP request to authenticate
        """
        for key, value in self.headers.items():
            request.headers[key] = value
