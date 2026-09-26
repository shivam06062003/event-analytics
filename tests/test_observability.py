from collections.abc import Iterator

import orjson
import pytest
from httpx import AsyncClient
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind
from prometheus_client import REGISTRY

from app.core.kafka import get_producer
from app.core.tracing import SAMPLER
from app.processor.processor import Processor
from tests.conftest import TEST_TOPIC, ProjectFixture, Reader, TopicCollector, drain, event
from tests.query_helpers import at, time_range

_exporter = InMemorySpanExporter()


@pytest.fixture(scope="module", autouse=True)
def tracer_provider() -> None:
    # A process can set the global provider once; spans are cleared per test.
    provider = TracerProvider(sampler=SAMPLER)
    provider.add_span_processor(SimpleSpanProcessor(_exporter))
    trace.set_tracer_provider(provider)


@pytest.fixture
def spans() -> Iterator[InMemorySpanExporter]:
    _exporter.clear()
    yield _exporter


def sample(name: str, **labels: str) -> float:
    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_metrics_endpoint_uses_route_templates(client: AsyncClient, reader: Reader) -> None:
    labels = {"method": "POST", "route": "/v1/query/segmentation", "status": "200"}
    before = sample("http_requests_total", **labels)

    await client.post(
        "/v1/query/segmentation",
        json={"event": "e", **time_range(at(0), at(1))},
        headers=reader.headers,
    )
    text = (await client.get("/metrics")).text

    assert sample("http_requests_total", **labels) == before + 1
    assert reader.project.id not in text  # no per-tenant series


async def test_ingest_metrics_count_events_by_outcome(
    client: AsyncClient, project: ProjectFixture
) -> None:
    accepted = sample("ingest_events_total", outcome="accepted")
    invalid = sample("ingest_events_total", outcome="rejected_validation")

    await client.post(
        "/v1/batch", json={"batch": [event(), event(), {"bad": 1}]}, headers=project.headers
    )

    assert sample("ingest_events_total", outcome="accepted") == accepted + 2
    assert sample("ingest_events_total", outcome="rejected_validation") == invalid + 1


async def test_processor_metrics(
    client: AsyncClient, project: ProjectFixture, processor: Processor
) -> None:
    await drain(processor)
    rows = sample("processor_rows_inserted_total")
    stored = sample("event_ingest_to_stored_seconds_count")
    dead = sample("processor_dead_letters_total", reason="invalid_json")

    await client.post("/v1/batch", json={"batch": [event(), event()]}, headers=project.headers)
    await get_producer().send_and_wait(TEST_TOPIC, b"not json", key=b"x")
    await drain(processor)

    assert sample("processor_rows_inserted_total") == rows + 2
    assert sample("event_ingest_to_stored_seconds_count") == stored + 2
    assert sample("processor_dead_letters_total", reason="invalid_json") == dead + 1
    lags = [
        s.value
        for metric in REGISTRY.collect()
        if metric.name == "processor_consumer_lag"
        for s in metric.samples
    ]
    assert lags and all(value == 0 for value in lags)  # caught up after draining


async def test_trace_context_travels_through_kafka_to_the_batch_span(
    client: AsyncClient,
    project: ProjectFixture,
    kafka: TopicCollector,
    processor: Processor,
    spans: InMemorySpanExporter,
) -> None:
    await drain(processor)
    tracer = trace.get_tracer("test")
    # Stand-in for the FastAPI server span that instrumentation creates in prod.
    with tracer.start_as_current_span("POST /v1/batch", kind=SpanKind.SERVER) as request_span:
        await client.post("/v1/batch", json={"batch": [event()]}, headers=project.headers)
    trace_id = format(request_span.get_span_context().trace_id, "032x")

    # 1. The Kafka message carries the request's trace context in its headers.
    [message] = await kafka.wait_for(project.id, 1)
    assert trace_id in message.headers["traceparent"]
    assert "traceparent" not in orjson.dumps(message.value).decode()  # payload untouched

    # 2. The processor's batch span LINKS back to it, with the insert as a child.
    await drain(processor)
    batch_spans = [
        s
        for s in spans.get_finished_spans()
        if s.name == "process events batch"
        and any(format(link.context.trace_id, "032x") == trace_id for link in s.links)
    ]
    assert len(batch_spans) == 1
    assert batch_spans[0].kind == SpanKind.CONSUMER
    children = [
        s for s in spans.get_finished_spans()
        if s.parent is not None and s.parent.span_id == batch_spans[0].context.span_id
    ]  # fmt: skip
    assert [c.name for c in children] == ["clickhouse insert"]
