# zeam-ai-poly

This is the Polylith workspace for the Zeam AI Popularity Recommender system.

## About Polylith Architecture

[Polylith](https://polylith.gitbook.io/) is a software architecture that applies functional thinking at the system scale.

It helps us build modular, reusable, and testable code by separating logic from configuration and assembly.

### Key Concepts

*   **Workspace**: The monorepo containing all your code (`zeam-ai-poly`).
*   **Components**: The building blocks. They contain the business logic and are completely decoupled from each other (except via interfaces).
    *   *Example*: `zeam.popularity` (core logic), `zeam.scheduler` (worker logic).
*   **Bases**: The entry points. They expose the components to the outside world (e.g., via a CLI, HTTP API, or Lambda). They contain very little logic, mostly just wiring.
    *   *Example*: `zeam.api` (FastAPI), `zeam.worker` (Celery Worker).
*   **Projects**: The deployable artifacts. Projects combine **bases** and **components** into a specific service configuration (e.g., a `pyproject.toml` that lists dependencies).
    *   *Example*: `projects/popularity-api` builds the API service.
*   **Development**: A special project (usually `development/zeam/dev`) that includes ALL components and bases, allowing you to work on the entire system in a single REPL/editor environment without context switching.

### Polylith CLI

You can visualize the workspace structure using the `poly` tool (installed via `polylith-cli`).

```bash
uv run poly info
```

```bash
uv run poly info
```

This will show you a matrix of which components and bases are used in which projects.

> **Note**: `poly info` visualizes dependencies based on **source code imports**. Since our deployable projects (e.g., `projects/popularity-api`) are configuration-only wrappers that rely on `pyproject.toml` dependencies, they may appear with empty dependencies (`-`) in the matrix. This is expected. The actual dependencies are enforced by `uv` during the build process.

## Project Structure

```
zeam-ai-poly/
├── bases/
│   └── zeam/
│       ├── api/         # FastAPI entry point
│       ├── worker/      # Celery Worker entry point
│       └── beat/        # Celery Beat entry point
├── components/
│   └── zeam/
│       ├── popularity/  # Core domain logic & database access
│       └── celery_core/      # Worker tasks & Celery core logic
├── projects/
│   ├── popularity-api/     # Deployable API service
│   ├── popularity-worker/  # Deployable Worker service
│   └── ...
├── development/
│   └── zeam/
│       └── dev/         # Development sandbox (REPL)
└── workspace.toml       # Polylith workspace config
```

## Getting Started

### Prerequisites

*   **Python 3.12+**
*   **[uv](https://docs.astral.sh/uv/)**: Fast Python package installer and resolver.
*   **Docker** (for deployment builds)

### Installation

Initialize the workspace and install dependencies:

```bash
uv sync
```

## Development

The best way to develop is using the Polylith development environment, which gives you access to all code in one place.

### Interactive REPL (IPython)

We have a pre-configured `dev` project for exploration.

```bash
# Using Makefile shortcut
make dev-repl

# Or manually
uv run --project development/zeam/dev ipython
```

Inside the REPL, you can import any component:

```python
from zeam.popularity.core import config
from zeam.redshift import config as redshift_config
from zeam.celery_core.workers.dummy_worker import DummyWorker


worker = DummyWorker()
# worker.process()
```

### Running Tests

Run the test suite using `pytest`. This runs tests for all components and bases.

```bash
# Using Makefile shortcut
make test

# Or manually
uv run --project development/zeam/dev pytest components/zeam/celery_core/tests
uv run --project development/zeam/dev pytest bases/zeam/api/tests
```

## Deployment

We use Docker for deployment. Each **Project** corresponds to a Docker image.

### Build Docker Images

Build all service images at once:

```bash
make build
```

This commands builds:
*   `popularity-api:latest`
*   `popularity-worker:latest`
*   `popularity-beat:latest`
*   `popularity-flower:latest`

### Run Services Locally

You can run individual services locally using `uv` and the specific project configuration (as shown above), or using `make` to run the Docker containers.

**Using Docker (via Makefile):**
```bash
# Run API (exposed on port 8000)
make run-api

# Run Worker
make run-worker

# Run Beat
make run-beat

# Run Flower (exposed on port 5555)
make run-flower
```

**Using uv (Native):**
**Run API:**
```bash
uv run --project projects/popularity-api uvicorn zeam.api.main:app --reload
```

**Run Worker:**
```bash
uv run --project projects/popularity-worker python -m zeam.worker
```
