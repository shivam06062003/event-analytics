.PHONY: install up down logs infra migrate project console send test lint format typecheck check

install:        ## Create venv and install app + dev tools
	python3.13 -m venv .venv
	.venv/bin/pip install -e ".[dev]"

up:             ## Build and start the stack (postgres, redpanda, migrations+topics, api on :8001)
	docker compose up --build -d

down:           ## Stop everything (data volumes are kept)
	docker compose --profile tools down

logs:           ## Tail API logs
	docker compose logs -f api

infra:          ## Start only Postgres + Redpanda (for running tests/app locally)
	docker compose up -d postgres redpanda

migrate:        ## Apply migrations and create Kafka topics (local)
	.venv/bin/alembic upgrade head
	.venv/bin/python -m app.cli ensure-topics

project:        ## Create a project and print its write key: make project name="My app"
	@docker compose exec -T api python -m app.cli create-project --name "$(or $(name),Demo app)"

console:        ## Redpanda Console (browse topics/messages) on http://localhost:8081
	docker compose --profile tools up -d console

send:           ## Send a sample batch: make send key=wk_...
	@curl -s localhost:8001/v1/batch -H "Authorization: Bearer $(key)" -H 'content-type: application/json' \
		-d '{"sent_at":"'$$(date -u +%Y-%m-%dT%H:%M:%SZ)'","batch":[{"event_id":"'$$(uuidgen)'","event":"page_viewed","anonymous_id":"anon-1","properties":{"path":"/pricing"}},{"event_id":"'$$(uuidgen)'","event":"signup","user_id":"u-1"}]}' ; echo

test:
	.venv/bin/pytest -v

lint:
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

format:
	.venv/bin/ruff check --fix .
	.venv/bin/ruff format .

typecheck:
	.venv/bin/mypy app

check: lint typecheck test   ## Everything CI runs
