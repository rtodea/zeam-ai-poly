FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY pyproject.toml uv.lock workspace.toml ./
COPY components ./components
COPY bases ./bases
COPY projects/popularity-worker ./projects/popularity-worker

RUN uv sync --frozen --project projects/popularity-worker

ENV PYTHONPATH=/app
ENV PATH="/app/projects/popularity-worker/.venv/bin:$PATH"

CMD ["uv", "run", "--project", "projects/popularity-worker", "celery", "-A", "zeam.scheduler.celery_app", "worker", "--loglevel=info"]
