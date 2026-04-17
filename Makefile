.PHONY: help install dev test test-cov lint format run clean docker-up docker-down docker-logs docker-ps

help: ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Development
install: ## Install dependencies
	uv sync

dev: ## Run development server with auto-reload
	uv run uvicorn dq_autofix.main:app --reload

run: ## Run production server
	uv run uvicorn dq_autofix.main:app --host 0.0.0.0 --port 8000

# Testing
test: ## Run tests
	uv run pytest -v

test-cov: ## Run tests with coverage
	uv run pytest --cov --cov-report=term-missing

test-fast: ## Run tests without integration tests
	uv run pytest -v -m "not integration"

# Code Quality
lint: ## Run linter
	uv run ruff check src/ tests/

format: ## Format code
	uv run ruff format src/ tests/

fix: ## Fix linting issues
	uv run ruff check --fix src/ tests/

# Docker - OpenMetadata
docker-up: ## Start OpenMetadata stack
	docker compose up -d

docker-down: ## Stop OpenMetadata stack
	docker compose down

docker-stop: ## Stop containers without removing
	docker compose stop

docker-start: ## Start stopped containers
	docker compose start

docker-logs: ## View OpenMetadata server logs
	docker compose logs -f openmetadata_server

docker-ps: ## Show running containers
	docker compose ps

docker-clean: ## Stop and remove containers with volumes
	docker compose down --volumes

# Utilities
clean: ## Clean up cache files
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name ".ruff_cache" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

env: ## Create .env from example
	cp -n .env.example .env || true
	@echo "Edit .env and add your OPENMETADATA_TOKEN"

check: ## Check if OpenMetadata API is accessible
	@curl -s http://localhost:8585/api/v1/system/version | head -c 100 && echo

api-check: ## Check if DQ AutoFix API is running
	@curl -s http://localhost:8000/api/v1/health | python -m json.tool
