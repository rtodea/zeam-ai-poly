.PHONY: api-local worker-local beat-local flower-local dev-repl build test clean sync

# Development commands
api-local:
	uv run --project projects/recommender-service uvicorn zeam.api.main:app --reload --port $${SERVER_PORT:-7311}

worker-local:
	uv run --project projects/recommender-background-service-worker celery -A zeam.worker.main worker --loglevel=info

beat-local:
	uv run --project projects/recommender-background-service-scheduler celery -A zeam.beat.main beat --loglevel=info

flower-local:
	uv run --project projects/recommender-background-service-monitor celery -A zeam.worker.main flower

repl:
	@uv sync --quiet
	PYTHONPATH=bases:components uv run --project development/zeam/dev ipython

sync:
	uv sync --project development/zeam/dev --reinstall

test:
	PYTHONPATH=bases:components uv run --project development/zeam/dev pytest components/zeam/analytics/tests bases/zeam/api/tests

tests: test

# Docker Build commands
build: build-api build-worker build-beat build-flower

build-api:
	docker build -t zeam-recommender-service -f projects/recommender-service/Dockerfile .

build-worker:
	docker build -t zeam-recommender-background-service-worker -f projects/recommender-background-service-worker/Dockerfile .

build-beat:
	docker build -t zeam-recommender-background-service-scheduler -f projects/recommender-background-service-scheduler/Dockerfile .

build-flower:
	docker build -t zeam-recommender-background-service-monitor -f projects/recommender-background-service-monitor/Dockerfile .

# Docker Run commands
docker-run-all: run-api run-worker run-beat run-flower redis-local

run-api:
	docker run --rm --env-file .env -p 7311:7311 zeam-recommender-service

run-worker:
	docker run --rm --env-file .env zeam-recommender-background-service-worker

run-beat:
	docker run --rm --env-file .env zeam-recommender-background-service-scheduler

run-flower:
	docker run --rm --env-file .env -p 5555:5555 zeam-recommender-background-service-monitor

redis-local:
	docker run --rm -d --name zeam-redis -p 127.0.0.1:6379:6379 redis:latest

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.py[co]" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ipynb_checkpoints" -exec rm -rf {} +
	find . -type f -name ".coverage" -delete
	