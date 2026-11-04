
.DEFAULT_GOAL := help

# Load environment variables from .env
ifneq (,$(wildcard .env))
    include .env
    export
endif

# Project
APP_SERVICE := application
APP_DIR := services/$(APP_SERVICE)
COMPOSE := docker-compose.yaml

# Go
GO := go
GOOSE := goose

# Database
MIGRATIONS_DIR := $(APP_DIR)/migrations


.PHONY: help
help: ## Show available commands
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z0-9_-]+:.*## / {printf "  %-24s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Go commands

.PHONY: fmt
fmt: ## Format Go code
	$(GO) -C $(APP_DIR) fmt ./...

.PHONY: vet
vet: ## Run go vet
	$(GO) -C $(APP_DIR) vet ./...

.PHONY: test
test: ## Run all tests
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" $(GO) -C $(APP_DIR) test ./...

.PHONY: test-race
test-race: ## Run tests with race detector
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" $(GO) -C $(APP_DIR) test -race ./...

.PHONY: test-cover
test-cover: ## Run tests with coverage check
	TEST_DATABASE_URL="$(TEST_DATABASE_URL)" $(GO) -C $(APP_DIR) test -cover ./...

.PHONY: build
build: ## Build application service
	$(GO) -C $(APP_DIR) build ./...

.PHONY: tidy
tidy: ## Tidy Go dependencies
	$(GO) -C $(APP_DIR) mod tidy

.PHONY: check
check: fmt vet test ## Run basic code checks

# Docker Compose

.PHONY: up
up: ## Start infrastructure
	docker compose -f $(COMPOSE) up -d

.PHONY: down
down: ## Stop infrastructure
	docker compose -f $(COMPOSE) down

.PHONY: restart
restart: ## Restart infrastructure
	docker compose -f $(COMPOSE) restart

.PHONY: logs
logs: ## Follow infrastructure logs
	docker compose -f $(COMPOSE) logs -f

.PHONY: ps
ps: ## Show running containers
	docker compose -f $(COMPOSE) ps

.PHONY: clean
clean: ## Stop containers and remove volumes
	docker compose -f $(COMPOSE) down -v

# Database migrations

.PHONY: migrate-up
migrate-up: ## Apply application migrations
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DATABASE_URL)" up

.PHONY: migrate-down
migrate-down: ## Roll back last application migration
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DATABASE_URL)" down

.PHONY: migrate-status
migrate-status: ## Show application migration status
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DATABASE_URL)" status

.PHONY: migrate-create
migrate-create: ## Create migration (usage: make migrate-create NAME=add_example)
	@test -n "$(NAME)" || (echo "Usage: make migrate-create NAME=migration_name"; exit 1)
	$(GOOSE) -dir $(MIGRATIONS_DIR) create $(NAME) sql
