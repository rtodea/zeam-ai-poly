FROM python:3.12-slim

WORKDIR /app

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/uv

# Copy workspace configuration
COPY pyproject.toml uv.lock workspace.toml ./

# Copy components and bases
COPY components ./components
COPY bases ./bases

# Copy project
COPY projects/popularity-api ./projects/popularity-api

# Install dependencies
RUN uv sync --frozen --project projects/popularity-api

# Expose port
EXPOSE 8000

# Set environment variables
ENV PYTHONPATH=/app
ENV PATH="/app/projects/popularity-api/.venv/bin:$PATH"

# Run the application
CMD ["uv", "run", "--project", "projects/popularity-api", "uvicorn", "zeam.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
