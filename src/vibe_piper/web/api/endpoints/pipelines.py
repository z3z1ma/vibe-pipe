"""Pipeline endpoints."""

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from vibe_piper.web.api.endpoints.auth import get_current_user

# =============================================================================
# Pydantic Models
# =============================================================================


class PipelineStatus(str):
    """Pipeline status enum."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PipelineResponse(BaseModel):
    """Pipeline response model."""

    id: str
    name: str
    description: str | None
    status: str
    created_at: datetime
    updated_at: datetime | None
    last_run_at: datetime | None
    last_run_status: str | None
    schedule: str | None
    is_active: bool


class PipelineListResponse(BaseModel):
    """Pipeline list response model."""

    pipelines: list[PipelineResponse]
    total: int
    page: int
    page_size: int


class PipelineRunResponse(BaseModel):
    """Pipeline run response model."""

    id: str
    pipeline_id: str
    status: str
    started_at: datetime
    completed_at: datetime | None
    duration_seconds: float | None
    error_message: str | None


class PipelineRunListResponse(BaseModel):
    """Pipeline run list response model."""

    runs: list[PipelineRunResponse]
    total: int
    page: int
    page_size: int


class AssetResponse(BaseModel):
    """Asset response model."""

    id: str
    name: str
    type: str
    location: str
    created_at: datetime
    updated_at: datetime | None
    size_bytes: int | None
    metadata: dict[str, Any] | None


class AssetListResponse(BaseModel):
    """Asset list response model."""

    assets: list[AssetResponse]
    total: int
    page: int
    page_size: int


# =============================================================================
# Router
# =============================================================================

router = APIRouter()


# =============================================================================
# Pipeline Endpoints
# =============================================================================


@router.get("", response_model=PipelineListResponse, tags=["pipelines"])
async def list_pipelines(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    status: str | None = Query(None, description="Filter by status"),
    is_active: bool | None = Query(None, description="Filter by active status"),
    current_user: dict = Depends(get_current_user),
) -> PipelineListResponse:
    """
    List all pipelines.

    Args:
        page: Page number
        page_size: Items per page
        status: Optional status filter
        is_active: Optional active status filter
        current_user: Current authenticated user

    Returns:
        Paginated list of pipelines
    """
    # TODO: Fetch pipelines from database
    # For now, return mock data

    mock_pipelines = [
        PipelineResponse(
            id="1",
            name="Data Import Pipeline",
            description="Imports data from external sources",
            status="completed",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            last_run_at=datetime.utcnow(),
            last_run_status="completed",
            schedule="0 6 * * *",
            is_active=True,
        ),
        PipelineResponse(
            id="2",
            name="Data Transformation Pipeline",
            description="Transforms imported data",
            status="running",
            created_at=datetime.utcnow(),
            updated_at=None,
            last_run_at=datetime.utcnow(),
            last_run_status="running",
            schedule=None,
            is_active=True,
        ),
    ]

    # Apply filters
    filtered = mock_pipelines
    if status:
        filtered = [p for p in filtered if p.status == status]
    if is_active is not None:
        filtered = [p for p in filtered if p.is_active == is_active]

    # Pagination
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    paginated = filtered[start:end]

    return PipelineListResponse(
        pipelines=paginated,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/{pipeline_id}", response_model=PipelineResponse, tags=["pipelines"])
async def get_pipeline(
    pipeline_id: str,
    current_user: dict = Depends(get_current_user),
) -> PipelineResponse:
    """
    Get pipeline by ID.

    Args:
        pipeline_id: Pipeline ID
        current_user: Current authenticated user

    Returns:
        Pipeline details

    Raises:
        HTTPException: If pipeline not found
    """
    # TODO: Fetch pipeline from database
    # For now, return mock data

    return PipelineResponse(
        id=pipeline_id,
        name="Data Import Pipeline",
        description="Imports data from external sources",
        status="completed",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        last_run_at=datetime.utcnow(),
        last_run_status="completed",
        schedule="0 6 * * *",
        is_active=True,
    )


# =============================================================================
# Pipeline Run Endpoints
# =============================================================================


@router.get("/{pipeline_id}/runs", response_model=PipelineRunListResponse, tags=["pipelines"])
async def list_pipeline_runs(
    pipeline_id: str,
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    status: str | None = Query(None, description="Filter by status"),
    current_user: dict = Depends(get_current_user),
) -> PipelineRunListResponse:
    """
    List runs for a pipeline.

    Args:
        pipeline_id: Pipeline ID
        page: Page number
        page_size: Items per page
        status: Optional status filter
        current_user: Current authenticated user

    Returns:
        Paginated list of pipeline runs
    """
    # TODO: Fetch pipeline runs from database
    # For now, return mock data

    mock_runs = [
        PipelineRunResponse(
            id="1",
            pipeline_id=pipeline_id,
            status="completed",
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            duration_seconds=120.5,
            error_message=None,
        ),
        PipelineRunResponse(
            id="2",
            pipeline_id=pipeline_id,
            status="failed",
            started_at=datetime.utcnow(),
            completed_at=datetime.utcnow(),
            duration_seconds=45.2,
            error_message="Connection timeout",
        ),
    ]

    # Apply filters
    filtered = mock_runs
    if status:
        filtered = [r for r in filtered if r.status == status]

    # Pagination
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    paginated = filtered[start:end]

    return PipelineRunListResponse(
        runs=paginated,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post("/{pipeline_id}/run", tags=["pipelines"])
async def run_pipeline(
    pipeline_id: str,
    current_user: dict = Depends(get_current_user),
) -> dict[str, str]:
    """
    Trigger a pipeline run.

    Args:
        pipeline_id: Pipeline ID
        current_user: Current authenticated user

    Returns:
        Run ID and status

    Raises:
        HTTPException: If pipeline not found or cannot be run
    """
    # TODO: Trigger pipeline execution
    # For now, return mock response

    run_id = "1"
    return {
        "run_id": run_id,
        "pipeline_id": pipeline_id,
        "status": "pending",
        "message": "Pipeline run queued",
    }


# =============================================================================
# Asset Endpoints
# =============================================================================


@router.get("/assets", response_model=AssetListResponse, tags=["assets"])
async def list_assets(
    page: int = Query(1, ge=1, description="Page number"),
    page_size: int = Query(50, ge=1, le=100, description="Items per page"),
    asset_type: str | None = Query(None, description="Filter by asset type"),
    current_user: dict = Depends(get_current_user),
) -> AssetListResponse:
    """
    List all assets.

    Args:
        page: Page number
        page_size: Items per page
        asset_type: Optional asset type filter
        current_user: Current authenticated user

    Returns:
        Paginated list of assets
    """
    # TODO: Fetch assets from database
    # For now, return mock data

    mock_assets = [
        AssetResponse(
            id="1",
            name="customer_data.csv",
            type="file",
            location="s3://bucket/customer_data.csv",
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow(),
            size_bytes=1024000,
            metadata={"source": "external_api", "rows": 10000},
        ),
        AssetResponse(
            id="2",
            name="analytics_db",
            type="database",
            location="postgresql://db/analytics",
            created_at=datetime.utcnow(),
            updated_at=None,
            size_bytes=None,
            metadata={"tables": 5},
        ),
    ]

    # Apply filters
    filtered = mock_assets
    if asset_type:
        filtered = [a for a in filtered if a.type == asset_type]

    # Pagination
    total = len(filtered)
    start = (page - 1) * page_size
    end = start + page_size
    paginated = filtered[start:end]

    return AssetListResponse(
        assets=paginated,
        total=total,
        page=page,
        page_size=page_size,
    )


@router.get("/assets/{asset_id}", response_model=AssetResponse, tags=["assets"])
async def get_asset(
    asset_id: str,
    current_user: dict = Depends(get_current_user),
) -> AssetResponse:
    """
    Get asset by ID.

    Args:
        asset_id: Asset ID
        current_user: Current authenticated user

    Returns:
        Asset details

    Raises:
        HTTPException: If asset not found
    """
    # TODO: Fetch asset from database
    # For now, return mock data

    return AssetResponse(
        id=asset_id,
        name="customer_data.csv",
        type="file",
        location="s3://bucket/customer_data.csv",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
        size_bytes=1024000,
        metadata={"source": "external_api", "rows": 10000},
    )
