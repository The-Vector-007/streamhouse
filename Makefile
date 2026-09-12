# One command per action. If you find yourself typing a long docker or spark
# incantation twice, it belongs in here.
SHELL := /bin/bash
PROFILE ?= ingest

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: setup
setup: ## install toolchain + python deps
	mise install
	uv sync

.PHONY: up
up: ## start containers for a profile (make up PROFILE=ingest)
	docker compose --profile $(PROFILE) up -d
	@echo "waiting for kafka to accept connections..."
	@until docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
	  --bootstrap-server localhost:9092 >/dev/null 2>&1; do sleep 1; done
	@echo "kafka ready"

.PHONY: down
down: ## stop everything and drop volumes
	docker compose --profile ingest --profile pipeline --profile serve --profile orchestrate down -v

.PHONY: topics
topics: ## list kafka topics
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

.PHONY: topics-create
topics-create: ## create topics txn.raw and txn.dlq with three parititions each
	uv run src/streamhouse/generator/topics.py


.PHONY: test
test: ## run python tests
	uv run pytest -q

.PHONY: test-java
test-java: ## run java tests (once services/ exist)
	@for d in services/*/; do \
	  if [ -f "$$d/pom.xml" ]; then echo "== $$d"; (cd $$d && mvn -q test); fi; \
	done

.PHONY: lint
lint: ## ruff check + format check
	uv run ruff check .
	uv run ruff format --check .

.PHONY: fmt
fmt: ## autoformat
	uv run ruff format .
	uv run ruff check --fix .

.PHONY: clean
clean: ## wipe local warehouse + checkpoints (destructive, local data only)
	rm -rf warehouse/ checkpoints/ data/
