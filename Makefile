.PHONY: api-local worker-local beat-local flower-local dev-repl build test clean

# Development commands
api-local:
	uv run --project projects/popularity-api uvicorn zeam.api.main:app --reload --port $${SERVER_PORT:-7311}

worker-local:
	uv run --project projects/popularity-worker celery -A zeam.worker.main worker --loglevel=info

beat-local:
	uv run --project projects/popularity-beat celery -A zeam.beat.main beat --loglevel=info

flower-local:
	uv run --project projects/popularity-flower celery -A zeam.worker.main flower

dev-repl:
	uv run --project development/zeam/dev ipython

test:
	uv run --project development/zeam/dev pytest components/zeam/analytics/tests
	uv run --project development/zeam/dev pytest bases/zeam/api/tests

tests: test

# Docker Build commands
build: build-api build-worker build-beat build-flower

build-api:
	docker build -t zeam-popularity-api -f projects/popularity-api/Dockerfile .

build-worker:
	docker build -t zeam-popularity-worker -f projects/popularity-worker/Dockerfile .

build-beat:
	docker build -t zeam-popularity-beat -f projects/popularity-beat/Dockerfile .

build-flower:
	docker build -t zeam-popularity-flower -f projects/popularity-flower/Dockerfile .

# Docker Run commands
docker-run-all: run-api run-worker run-beat run-flower redis-local

run-api:
	docker run --rm --env-file .env -p 7311:7311 zeam-popularity-api

run-worker:
	docker run --rm --env-file .env zeam-popularity-worker

run-beat:
	docker run --rm --env-file .env zeam-popularity-beat

run-flower:
	docker run --rm --env-file .env -p 5555:5555 zeam-popularity-flower

redis-local:
	docker run --rm -d --name zeam-redis -p 127.0.0.1:6379:6379 redis:latest

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.py[co]" -delete
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ipynb_checkpoints" -exec rm -rf {} +
	find . -type f -name ".coverage" -delete
	