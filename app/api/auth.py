import uuid
from typing import Annotated

import structlog
from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.services import projects as project_service
from app.services.errors import Forbidden, Unauthenticated

_bearer = HTTPBearer(auto_error=False, description="Write key (`wk_...`) or read key (`rk_...`)")


async def get_project_from_write_key(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> uuid.UUID:
    if credentials is None:
        raise Unauthenticated("Missing write key. Send it as 'Authorization: Bearer <key>'.")
    project_id = await project_service.authenticate_write_key(credentials.credentials)
    structlog.contextvars.bind_contextvars(project_id=str(project_id))
    return project_id


async def get_project_from_read_key(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> uuid.UUID:
    if credentials is None:
        raise Unauthenticated("Missing read key. Send it as 'Authorization: Bearer <key>'.")
    principal = await project_service.authenticate_read_key(credentials.credentials)
    structlog.contextvars.bind_contextvars(project_id=str(principal.project_id))
    return principal.project_id


async def get_project_for_management(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> uuid.UUID:
    if credentials is None:
        raise Unauthenticated("Missing read key. Send it as 'Authorization: Bearer <key>'.")
    principal = await project_service.authenticate_read_key(credentials.credentials)
    if not principal.can_manage:
        raise Forbidden("This read key cannot change project settings (create one with --manage)")
    structlog.contextvars.bind_contextvars(project_id=str(principal.project_id))
    return principal.project_id


WriteKeyProject = Annotated[uuid.UUID, Depends(get_project_from_write_key)]
ReadKeyProject = Annotated[uuid.UUID, Depends(get_project_from_read_key)]
ManagerProject = Annotated[uuid.UUID, Depends(get_project_for_management)]
