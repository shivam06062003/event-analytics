import uuid
from typing import Annotated

import structlog
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.services import projects as project_service
from app.services.errors import Unauthenticated

_bearer = HTTPBearer(auto_error=False, description="Project write key: `Bearer wk_...`")


async def get_project_from_write_key(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> uuid.UUID:
    if credentials is None:
        raise Unauthenticated("Missing write key. Send it as 'Authorization: Bearer <key>'.")
    project_id = await project_service.authenticate_write_key(credentials.credentials)
    structlog.contextvars.bind_contextvars(project_id=str(project_id))
    return project_id


WriteKeyProject = Annotated[uuid.UUID, Depends(get_project_from_write_key)]
