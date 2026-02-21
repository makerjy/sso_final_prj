SHELL := /bin/bash

.PHONY: bootstrap smoke-local test-viz up down

bootstrap:
	./scripts/bootstrap_dev.sh

smoke-local:
	./scripts/smoke_local.sh

test-viz:
	cd query-visualization && .venv/bin/python -m pytest -q

up:
	docker compose up -d --build

down:
	docker compose down
