import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, ForeignKey, String, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.models.base import Base


class Project(Base):
    """A tenant: one product (app or website) sending events."""

    __tablename__ = "projects"
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class WriteKey(Base):
    """Credential for SENDING events to one project.

    Write keys are embedded in apps and web pages, so they must be treated as
    semi-public: they can only append events to their own project, never read
    data. (Read access gets separate keys in Phase 3.) Stored as a SHA-256 hash.
    """

    __tablename__ = "write_keys"
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id"), index=True)
    prefix: Mapped[str] = mapped_column(String(16))
    key_hash: Mapped[str] = mapped_column(String(64), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ReadKey(Base):
    """Credential for QUERYING one project's data.

    Separate from write keys on purpose: write keys ship inside apps and web
    pages (effectively public), so they must never be able to read anything.
    Read keys live on servers and dashboards only. Never cached, so revoking
    one takes effect immediately.
    """

    __tablename__ = "read_keys"
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id"), index=True)
    prefix: Mapped[str] = mapped_column(String(16))
    key_hash: Mapped[str] = mapped_column(String(64), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    # Changing ingestion rules (tracking plans) is more than reading data, so
    # it needs an explicitly privileged read key.
    can_manage: Mapped[bool] = mapped_column(default=False, server_default=text("false"))


class TrackingPlan(Base):
    """One version of a project's tracking plan. Append-only: every change is
    a new version, so you can see exactly what the rules were at any time."""

    __tablename__ = "tracking_plans"
    __table_args__ = (UniqueConstraint("project_id", "version"),)
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id"))
    version: Mapped[int]
    plan: Mapped[dict[str, Any]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
