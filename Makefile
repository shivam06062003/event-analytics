.PHONY: install up down logs infra migrate project console send lag events ch test lint format typecheck check read-key seed

install:        ## Create venv and install app + dev tools
	python3.13 -m venv .venv
	.venv/bin/pip install -e ".[dev]"

up:             ## Build and start the stack (postgres, redpanda, migrations+topics, api on :8001)
	docker compose up --build -d

down:           ## Stop everything (data volumes are kept)
	docker compose --profile tools down

logs:           ## Tail API logs
	docker compose logs -f api

infra:          ## Start only the backing services (for running tests/app locally)
	docker compose up -d postgres redpanda clickhouse redis

migrate:        ## Apply Postgres + ClickHouse migrations and create Kafka topics (local)
	.venv/bin/alembic upgrade head
	.venv/bin/python -m app.cli ensure-topics
	.venv/bin/python -m app.cli clickhouse-migrate

project:        ## Create a project and print its write key: make project name="My app"
	@docker compose exec -T api python -m app.cli create-project --name "$(or $(name),Demo app)"

console:        ## Redpanda Console (browse topics/messages) on http://localhost:8081
	docker compose --profile tools up -d console

send:           ## Send a sample batch: make send key=wk_...
	@curl -s localhost:8001/v1/batch -H "Authorization: Bearer $(key)" -H 'content-type: application/json' \
		-d '{"sent_at":"'$$(date -u +%Y-%m-%dT%H:%M:%SZ)'","batch":[{"event_id":"'$$(uuidgen)'","event":"page_viewed","anonymous_id":"anon-1","properties":{"path":"/pricing"}},{"event_id":"'$$(uuidgen)'","event":"signup","user_id":"u-1"}]}' ; echo

lag:            ## Consumer lag per partition (how far the processor is behind)
	docker compose exec -T redpanda rpk group describe event-processor

events:         ## Event counts in ClickHouse (FINAL = after dedup)
	@docker compose exec -T clickhouse clickhouse-client --user analytics --password analytics -q \
		"SELECT event, count() AS events, uniqExact(distinct_id) AS users FROM analytics.events FINAL GROUP BY event ORDER BY events DESC FORMAT PrettyCompact"

ch:             ## Interactive ClickHouse SQL shell
	docker compose exec clickhouse clickhouse-client --user analytics --password analytics -d analytics

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

read-key:       ## Create a read (query) key: make read-key project=<project id>
	@docker compose exec -T api python -m app.cli create-read-key --project "$(project)"

seed:           ## Demo data straight into ClickHouse: make seed project=<id> [users=200000]
	@docker compose exec -T api python -m app.cli seed-demo --project "$(project)" --users $(or $(users),200000)
