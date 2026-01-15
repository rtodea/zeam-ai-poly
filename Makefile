.PHONY: api-local worker-local beat-local flower-local dev-repl build test

# Development commands
api-local:
	uv run --project projects/popularity-api uvicorn zeam.api.main:app --reload --port $${SERVER_PORT:-7311}

worker-local:
	uv run --project projects/popularity-worker celery -A zeam.scheduler.celery_app worker --loglevel=info

beat-local:
	uv run --project projects/popularity-beat celery -A zeam.scheduler.celery_app beat --loglevel=info

flower-local:
	uv run --project projects/popularity-flower celery -A zeam.scheduler.celery_app flower

dev-repl:
	uv run --project development/zeam/dev ipython

test:
	uv run --project development/zeam/dev pytest components/zeam/scheduler/tests
	uv run --project development/zeam/dev pytest bases/zeam/api/tests

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
