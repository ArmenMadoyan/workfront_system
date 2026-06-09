# Image for the one-shot migration runner (compose `migrate` service).
FROM python:3.12-slim

WORKDIR /app
COPY libs/wf_core /app/wf_core
RUN pip install --no-cache-dir ./wf_core

WORKDIR /app/wf_core
CMD ["alembic", "upgrade", "head"]