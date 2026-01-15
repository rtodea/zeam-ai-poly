FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY pyproject.toml uv.lock workspace.toml ./
COPY components ./components
COPY bases ./bases
COPY projects/popularity-beat ./projects/popularity-beat

RUN uv sync --frozen --project projects/popularity-beat

ENV PYTHONPATH=/app
ENV PATH="/app/projects/popularity-beat/.venv/bin:$PATH"

CMD ["uv", "run", "--project", "projects/popularity-beat", "celery", "-A", "zeam.scheduler.celery_app", "beat", "--loglevel=info"]
