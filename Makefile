.DEFAULT_GOAL := help
COMPOSE := docker compose
AF := $(COMPOSE) exec airflow-scheduler
DAG := quickbite_daily_warehouse
SEVEN_DAYS_AGO := $(shell date -v-7d +%F 2>/dev/null || date -d '7 days ago' +%F)
HOST_UID := $(shell [ "$$(uname)" = "Linux" ] && id -u || echo 50000)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

.env:
	@cp .env.example .env
	@sed -i.bak "s/^QB_START_DATE=.*/QB_START_DATE=$(SEVEN_DAYS_AGO)/; s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(HOST_UID)/" .env && rm -f .env.bak
	@echo "created .env (QB_START_DATE=$(SEVEN_DAYS_AGO))"

# ------------------------------------------------------------------ lifecycle
up: .env ## Build and start Airflow; it back-fills every day since QB_START_DATE
	$(COMPOSE) up -d --build
	@echo ""
	@echo "  Airflow UI: http://localhost:8080   (user: airflow  password: airflow)"
	@echo "  The DAG starts back-filling within a minute. Watch it in Grid view."

down: ## Stop everything (data kept)
	$(COMPOSE) down

clean: ## Stop and DELETE everything (warehouse, Airflow history)
	$(COMPOSE) --profile docs down -v --remove-orphans
	rm -rf transform/target transform/dbt_packages transform/logs

ps: ## Container status
	$(COMPOSE) ps

logs: ## Follow scheduler logs
	$(COMPOSE) logs -f --tail=100 airflow-scheduler

# ------------------------------------------------------------------ operate
runs: ## List DAG runs and their state
	$(AF) airflow dags list-runs -d $(DAG)

rerun: ## Re-run one day, e.g. `make rerun DAY=2026-09-25` (proves idempotency)
	@test -n "$(DAY)" || (echo "usage: make rerun DAY=YYYY-MM-DD" && exit 1)
	$(AF) airflow tasks clear $(DAG) -s $(DAY) -e $(DAY) -y

sql: ## Query the warehouse: `make sql` (interactive) or `make sql Q="select ..."`
ifdef Q
	$(AF) python -m quickbite.sql_shell "$(Q)"
else
	$(COMPOSE) exec -it airflow-scheduler python -i -m quickbite.sql_shell
endif

docs: ## dbt docs with the lineage graph at http://localhost:8081 (Ctrl+C to stop)
	$(COMPOSE) --profile docs up dbt-docs

cost-report: ## BigQuery only: bytes scanned, partitioned vs unpartitioned
	$(AF) python -m quickbite.bq_cost_report

# ------------------------------------------------------------------ develop
install-dev: ## Install local dev dependencies (no Docker, no Airflow)
	pip install -r requirements/dev.txt

local: ## Run the pipeline for 7 days locally without Airflow (DuckDB)
	python -m quickbite.run_local --days 7

test: ## Unit tests (pure Python)
	pytest -q

lint: ## Lint Python
	ruff check .

.PHONY: help up down clean ps logs runs rerun sql docs cost-report install-dev local test lint
