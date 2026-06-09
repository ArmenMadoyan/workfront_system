"""OpenTelemetry tracing setup + W3C trace-context propagation.

The hard part of this system is that one logical request crosses an async
boundary: API request -> outbox row -> Debezium -> Kafka -> worker. To keep it
on ONE trace, the API injects the W3C `traceparent` into the outbox payload;
the worker extracts it and continues the same trace as a child span.

Usage:
    from wf_core.telemetry import setup_tracing, inject_trace, extract_context, tracer
    setup_tracing("workfront-api")           # once, at startup
    carrier = inject_trace({})               # -> {"traceparent": "..."} into outbox payload
    ctx = extract_context(payload["_trace"]) # worker side
    with tracer().start_as_current_span("worker.cascade", context=ctx): ...
"""

from __future__ import annotations

import os

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

_PROPAGATOR = TraceContextTextMapPropagator()
_initialized = False


def setup_tracing(service_name: str) -> None:
    global _initialized
    if _initialized:
        return
    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint, insecure=True)))
    trace.set_tracer_provider(provider)
    _initialized = True


def inject_trace(carrier: dict) -> dict:
    """Write the current span's W3C traceparent into `carrier` (e.g. an outbox payload)."""
    _PROPAGATOR.inject(carrier)
    return carrier


def extract_context(carrier: dict):
    """Rebuild the parent context from a carrier produced by inject_trace()."""
    return _PROPAGATOR.extract(carrier or {})


def tracer(name: str = "wf"):
    return trace.get_tracer(name)