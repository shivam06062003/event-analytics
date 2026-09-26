#!/bin/sh
# API entrypoint. Runs $WEB_CONCURRENCY uvicorn worker processes (uvicorn reads
# that variable itself): Python executes one thread at a time per process, so
# using several cores needs several processes.
set -e
if [ -n "$PROMETHEUS_MULTIPROC_DIR" ]; then
    # Each process writes its metrics to files here; /metrics merges them.
    # Must start empty, or counters from a previous run would be double counted.
    rm -rf "$PROMETHEUS_MULTIPROC_DIR"
    mkdir -p "$PROMETHEUS_MULTIPROC_DIR"
fi
exec uvicorn app.main:app --host 0.0.0.0 --port 8000 --no-access-log "$@"
