"""OpenTelemetry tracing, including across Kafka.

The API puts the W3C `traceparent` of the ingestion request into each Kafka
message's HEADERS (headers are metadata, so the event payload is untouched).
The processor, however, handles a BATCH of messages that came from many
different requests at once. A span can only have one parent, so the batch span
instead carries LINKS to each producer context: the standard OpenTelemetry
pattern for batch consumers. In Jaeger you can jump from a request to the
batch that stored its events, and back.
"""

from collections.abc import Sequence

from fastapi import FastAPI
from opentelemetry import propagate, trace
from opentelemetry.context import Context
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import Decision, ParentBased, Sampler, SamplingResult
from opentelemetry.trace import Link, SpanKind
from opentelemetry.util.types import Attributes

from app.core.config import Settings

tracer = trace.get_tracer("event-analytics")
MAX_LINKS_PER_BATCH = 64


class EntryPointSpansOnly(Sampler):
    """New traces start only at SERVER (API request) or CONSUMER (processed
    batch) spans. Background polling queries would otherwise each start a
    root trace and drown the useful ones (lesson from the ledger project)."""

    def should_sample(
        self,
        parent_context: Context | None,
        trace_id: int,
        name: str,
        kind: SpanKind | None = None,
        attributes: Attributes = None,
        links: Sequence[Link] | None = None,
        trace_state: trace.TraceState | None = None,
    ) -> SamplingResult:
        if kind in (SpanKind.SERVER, SpanKind.CONSUMER):
            return SamplingResult(Decision.RECORD_AND_SAMPLE, attributes, trace_state)
        return SamplingResult(Decision.DROP, None, trace_state)

    def get_description(self) -> str:
        return "EntryPointSpansOnly"


SAMPLER = ParentBased(root=EntryPointSpansOnly())


def configure_tracing(settings: Settings, app: FastAPI | None = None) -> None:
    if not settings.otel_enabled:
        return
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

    from app.core.db import engine

    provider = TracerProvider(
        resource=Resource.create({"service.name": settings.otel_service_name}), sampler=SAMPLER
    )
    provider.add_span_processor(
        BatchSpanProcessor(OTLPSpanExporter(endpoint=settings.otel_exporter_otlp_endpoint))
    )
    trace.set_tracer_provider(provider)
    SQLAlchemyInstrumentor().instrument(engine=engine.sync_engine)
    if app is not None:
        FastAPIInstrumentor.instrument_app(app, excluded_urls="health/.*,metrics")


def kafka_headers() -> list[tuple[str, bytes]]:
    """The current trace context as Kafka message headers (empty if untraced)."""
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return [(key, value.encode()) for key, value in carrier.items()]


def links_from_headers(all_headers: Sequence[Sequence[tuple[str, bytes]]]) -> list[Link]:
    """Distinct producer contexts of a batch, as span links (capped)."""
    links: list[Link] = []
    seen: set[int] = set()
    for headers in all_headers:
        carrier = {key: value.decode() for key, value in headers if key == "traceparent"}
        if not carrier:
            continue
        context = trace.get_current_span(propagate.extract(carrier)).get_span_context()
        if context.is_valid and context.span_id not in seen:
            seen.add(context.span_id)
            links.append(Link(context))
            if len(links) >= MAX_LINKS_PER_BATCH:
                break
    return links
