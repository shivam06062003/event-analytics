from datetime import UTC, datetime

from fastapi import APIRouter, Request, status

from app.api.auth import WriteKeyProject
from app.core.kafka import get_producer
from app.schemas.events import BatchRequest, BatchResponse
from app.services import ingest as ingest_service

router = APIRouter(prefix="/v1", tags=["ingestion"])


@router.post("/batch", status_code=status.HTTP_202_ACCEPTED)
async def ingest_batch(
    body: BatchRequest, request: Request, project_id: WriteKeyProject
) -> BatchResponse:
    """Accept a batch of events for asynchronous processing.

    202 means every accepted event is durably stored in the event log; it will
    be queryable shortly after. Invalid events are listed in `rejected` and
    must not be retried unchanged. On 503, retry the SAME batch (same
    event_ids) after Retry-After: duplicates are removed downstream.
    """
    received_at = datetime.now(UTC)
    result = await ingest_service.ingest_batch(
        get_producer(),
        project_id,
        body,
        received_at=received_at,
        ip=request.client.host if request.client else None,
    )
    return BatchResponse(accepted=result.accepted, rejected=result.rejected)
