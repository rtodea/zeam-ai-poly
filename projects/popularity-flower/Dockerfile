FROM python:3.12-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

COPY pyproject.toml uv.lock workspace.toml ./
COPY components ./components
COPY bases ./bases
COPY projects/popularity-flower ./projects/popularity-flower

RUN uv sync --frozen --project projects/popularity-flower

EXPOSE 5555

ENV PYTHONPATH=/app
ENV PATH="/app/projects/popularity-flower/.venv/bin:$PATH"

CMD ["uv", "run", "--project", "projects/popularity-flower", "celery", "-A", "zeam.scheduler.celery_app", "flower"]
