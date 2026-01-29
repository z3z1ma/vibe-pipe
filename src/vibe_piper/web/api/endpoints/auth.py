"""Authentication endpoints."""

from datetime import datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel, EmailStr, Field

from vibe_piper.web.middleware.error_handler import HTTPExceptionWithDetail

# Router
router = APIRouter()

# Configuration
SECRET_KEY = "your-secret-key-change-in-production"  # TODO: Load from env
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS = 7

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login")


# =============================================================================
# Pydantic Models
# =============================================================================


class LoginRequest(BaseModel):
    """Login request model."""

    username: str = Field(..., min_length=1, description="Username or email")
    password: str = Field(..., min_length=1, description="Password")


class LoginResponse(BaseModel):
    """Login response model with tokens."""

    access_token: str = Field(..., description="JWT access token")
    refresh_token: str = Field(..., description="JWT refresh token")
    token_type: str = Field(default="bearer", description="Token type")
    expires_in: int = Field(..., description="Access token expiry in seconds")


class RegisterRequest(BaseModel):
    """User registration request model."""

    email: EmailStr = Field(..., description="User email")
    username: str = Field(..., min_length=3, max_length=50, description="Username")
    password: str = Field(..., min_length=8, max_length=100, description="Password")
    full_name: str | None = Field(None, description="Full name")


class UserResponse(BaseModel):
    """User information response."""

    id: str
    email: str
    username: str
    full_name: str | None
    is_active: bool
    created_at: datetime


class RefreshTokenRequest(BaseModel):
    """Refresh token request model."""

    refresh_token: str = Field(..., description="Refresh token")


# =============================================================================
# Token Management
# =============================================================================


def create_access_token(data: dict[str, Any], expires_delta: timedelta | None = None) -> str:
    """
    Create JWT access token.

    Args:
        data: Data to encode in token
        expires_delta: Optional custom expiry time

    Returns:
        Encoded JWT token
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire, "type": "access"})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def create_refresh_token(data: dict[str, Any]) -> str:
    """
    Create JWT refresh token.

    Args:
        data: Data to encode in token

    Returns:
        Encoded JWT refresh token
    """
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)
    to_encode.update({"exp": expire, "type": "refresh"})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def verify_token(token: str) -> dict[str, Any]:
    """
    Verify and decode JWT token.

    Args:
        token: JWT token string

    Returns:
        Decoded token data

    Raises:
        HTTPException: If token is invalid
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

        # Check token type
        token_type = payload.get("type")
        if token_type not in ("access", "refresh"):
            raise HTTPExceptionWithDetail(
                status_code=status.HTTP_401_UNAUTHORIZED,
                error="invalid_token",
                message="Invalid token type",
            )

        return payload

    except JWTError as e:
        raise HTTPExceptionWithDetail(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error="invalid_token",
            message=f"Invalid token: {e}",
        ) from e


# =============================================================================
# Dependency for Current User
# =============================================================================


async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict[str, Any]:
    """
    Get current authenticated user from JWT token.

    Args:
        token: JWT access token

    Returns:
        User data from token

    Raises:
        HTTPException: If token is invalid
    """
    payload = verify_token(token)

    # Extract user ID
    user_id = payload.get("sub")
    if user_id is None:
        raise HTTPExceptionWithDetail(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error="invalid_token",
            message="User not found in token",
        )

    # TODO: Load full user from database
    # For now, return minimal user data from token
    return {
        "id": user_id,
        "email": payload.get("email"),
        "username": payload.get("username"),
    }


# =============================================================================
# Endpoints
# =============================================================================


@router.post("/login", response_model=LoginResponse, tags=["auth"])
async def login(form_data: OAuth2PasswordRequestForm = Depends()) -> LoginResponse:
    """
    Login with username/password.

    Returns JWT access and refresh tokens.

    Args:
        form_data: OAuth2 password form data

    Returns:
        Login response with tokens

    Raises:
        HTTPExceptionWithDetail: If credentials are invalid
    """
    # TODO: Verify user credentials from database
    # For now, accept any non-empty username/password for development
    if not form_data.username or not form_data.password:
        raise HTTPExceptionWithDetail(
            status_code=status.HTTP_400_BAD_REQUEST,
            error="invalid_credentials",
            message="Username and password are required",
        )

    # Simulate user lookup (TODO: replace with database lookup)
    user = {
        "id": "1",
        "email": f"{form_data.username}@example.com",
        "username": form_data.username,
    }

    # Create tokens
    access_token = create_access_token(
        data={
            "sub": user["id"],
            "email": user["email"],
            "username": user["username"],
        },
    )

    refresh_token = create_refresh_token(
        data={
            "sub": user["id"],
            "email": user["email"],
            "username": user["username"],
        },
    )

    return LoginResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    )


@router.post("/logout", tags=["auth"])
async def logout(current_user: dict = Depends(get_current_user)) -> dict[str, str]:
    """
    Logout user.

    In a production system, this would invalidate the token on the server side.
    For JWT tokens, the client simply discards the token.

    Args:
        current_user: Current authenticated user

    Returns:
        Logout confirmation
    """
    # TODO: Add token to blacklist for server-side invalidation
    return {"message": "Successfully logged out"}


@router.post(
    "/register", response_model=UserResponse, status_code=status.HTTP_201_CREATED, tags=["auth"]
)
async def register(request: RegisterRequest) -> UserResponse:
    """
    Register a new user.

    Args:
        request: Registration request data

    Returns:
        Created user information

    Raises:
        HTTPExceptionWithDetail: If user already exists or validation fails
    """
    # TODO: Check if user already exists
    # TODO: Hash password and store in database
    # TODO: Send verification email

    # Simulate user creation (TODO: replace with database insertion)
    # hashed_password = pwd_context.hash(request.password)

    user = UserResponse(
        id="1",
        email=request.email,
        username=request.username,
        full_name=request.full_name,
        is_active=True,
        created_at=datetime.utcnow(),
    )

    return user


@router.post("/refresh", response_model=LoginResponse, tags=["auth"])
async def refresh_token(request: RefreshTokenRequest) -> LoginResponse:
    """
    Refresh access token using refresh token.

    Args:
        request: Refresh token request

    Returns:
        New access and refresh tokens

    Raises:
        HTTPExceptionWithDetail: If refresh token is invalid
    """
    payload = verify_token(request.refresh_token)

    # Check token type
    if payload.get("type") != "refresh":
        raise HTTPExceptionWithDetail(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error="invalid_token",
            message="Refresh token required",
        )

    # Extract user data
    user_id = payload.get("sub")
    email = payload.get("email")
    username = payload.get("username")

    if user_id is None:
        raise HTTPExceptionWithDetail(
            status_code=status.HTTP_401_UNAUTHORIZED,
            error="invalid_token",
            message="User not found in token",
        )

    # Create new tokens
    access_token = create_access_token(
        data={
            "sub": user_id,
            "email": email,
            "username": username,
        },
    )

    refresh_token = create_refresh_token(
        data={
            "sub": user_id,
            "email": email,
            "username": username,
        },
    )

    return LoginResponse(
        access_token=access_token,
        refresh_token=refresh_token,
        token_type="bearer",
        expires_in=ACCESS_TOKEN_EXPIRE_MINUTES * 60,
    )


@router.get("/me", response_model=UserResponse, tags=["auth"])
async def get_current_user_info(current_user: dict = Depends(get_current_user)) -> UserResponse:
    """
    Get current authenticated user information.

    Args:
        current_user: Current authenticated user

    Returns:
        User information
    """
    # TODO: Return full user data from database
    return UserResponse(
        id=current_user["id"],
        email=current_user["email"] or "",
        username=current_user["username"] or "",
        full_name=None,
        is_active=True,
        created_at=datetime.utcnow(),
    )


# TODO: OAuth2 endpoints for Google, GitHub, etc.
# @router.get("/oauth/google", tags=["auth"])
# async def google_oauth_redirect():
#     """Redirect to Google OAuth."""
#     pass
#
# @router.get("/oauth/google/callback", tags=["auth"])
# async def google_oauth_callback():
#     """Handle Google OAuth callback."""
#     pass
