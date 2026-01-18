# FlashQ Makefile
# ===============

.PHONY: dev run server release test postgres up down logs clean dashboard stress restart stop fmt lint check help

# Configuration (override with environment variables)
DATABASE_URL ?= postgres://flashq:flashq@localhost:5432/flashq
HTTP_PORT ?= 6790
TCP_PORT ?= 6789
PIDFILE := /tmp/flashq.pid

# =============
# Development
# =============

dev:
	cd server && cargo run

run:
	cd server && HTTP=1 cargo run

server:
	cd server && HTTP=1 GRPC=1 cargo run --release

release:
	cd server && HTTP=1 cargo run --release

# =============
# Code Quality
# =============

fmt:
	cd server && cargo fmt

lint:
	cd server && cargo clippy -- -D warnings

check:
	cd server && cargo fmt --check && cargo clippy -- -D warnings
	@echo "Code quality checks passed"

test:
	cd server && cargo test

# =============
# Docker
# =============

postgres:
	docker-compose up -d postgres

up:
	docker-compose up -d

down:
	docker-compose down

logs:
	docker-compose logs -f

docker-build:
	docker build -t flashq .

# =============
# Server Management
# =============

persist: up
	cd server && DATABASE_URL=$(DATABASE_URL) HTTP=1 cargo run --release

stop:
	@if [ -f $(PIDFILE) ]; then \
		kill $$(cat $(PIDFILE)) 2>/dev/null || true; \
		rm -f $(PIDFILE); \
		echo "Server stopped"; \
	else \
		pkill -f "target/release/flashq-server" 2>/dev/null || echo "Server not running"; \
	fi

restart: stop up
	@sleep 1
	@cd server && DATABASE_URL=$(DATABASE_URL) HTTP=1 \
		nohup cargo run --release > /tmp/flashq.log 2>&1 & echo $$! > $(PIDFILE)
	@echo "Waiting for server..."
	@for i in $$(seq 1 30); do \
		curl -s http://localhost:$(HTTP_PORT)/health > /dev/null 2>&1 && break; \
		sleep 1; \
	done
	@curl -s http://localhost:$(HTTP_PORT)/health > /dev/null 2>&1 || \
		(echo "Server failed to start" && cat /tmp/flashq.log && exit 1)
	@echo "Server ready at http://localhost:$(HTTP_PORT)"

restart-mem: stop
	@sleep 1
	@cd server && HTTP=1 \
		nohup cargo run --release > /tmp/flashq.log 2>&1 & echo $$! > $(PIDFILE)
	@echo "Waiting for server..."
	@for i in $$(seq 1 15); do \
		curl -s http://localhost:$(HTTP_PORT)/health > /dev/null 2>&1 && break; \
		sleep 1; \
	done
	@echo "Server restarted (memory only)"
	@echo "Dashboard: http://localhost:$(HTTP_PORT)"

# =============
# Testing
# =============

sdk-test:
	cd sdk/typescript && bun run examples/comprehensive-test.ts

stress:
	cd sdk/typescript && bun run examples/stress-test.ts

full-test: up
	@echo "Starting server..."
	@cd server && DATABASE_URL=$(DATABASE_URL) HTTP=1 \
		nohup cargo run --release > /tmp/flashq.log 2>&1 & echo $$! > $(PIDFILE)
	@echo "Waiting for server..."
	@for i in $$(seq 1 60); do \
		curl -s http://localhost:$(HTTP_PORT)/health > /dev/null 2>&1 && break; \
		sleep 1; \
	done
	@curl -s http://localhost:$(HTTP_PORT)/health > /dev/null 2>&1 || \
		(echo "Server failed to start" && cat /tmp/flashq.log && exit 1)
	@echo "Server ready, running tests..."
	cd sdk/typescript && bun run examples/comprehensive-test.ts
	@$(MAKE) stop
	@echo "Full test completed"

# =============
# Utilities
# =============

dashboard:
ifeq ($(shell uname),Darwin)
	open http://localhost:$(HTTP_PORT)
else ifeq ($(shell uname),Linux)
	xdg-open http://localhost:$(HTTP_PORT) 2>/dev/null || echo "Open http://localhost:$(HTTP_PORT)"
else
	@echo "Open http://localhost:$(HTTP_PORT) in your browser"
endif

clean:
	cd server && cargo clean
	docker-compose down -v
	rm -f $(PIDFILE) /tmp/flashq.log

# =============
# Help
# =============

help:
	@echo "FlashQ Commands"
	@echo ""
	@echo "Server:"
	@echo "  make server      Run release server (HTTP + gRPC, in-memory)"
	@echo "  make persist     Run with PostgreSQL persistence"
	@echo "  make dev         Run server in dev mode"
	@echo "  make stop        Stop running server"
	@echo "  make restart     Restart server with PostgreSQL"
	@echo "  make restart-mem Restart server (memory only)"
	@echo ""
	@echo "Code Quality:"
	@echo "  make fmt         Format code"
	@echo "  make lint        Run clippy"
	@echo "  make check       Run fmt --check + clippy"
	@echo ""
	@echo "Docker:"
	@echo "  make up          Start PostgreSQL container"
	@echo "  make down        Stop all containers"
	@echo "  make logs        View container logs"
	@echo "  make docker-build Build Docker image"
	@echo ""
	@echo "Testing:"
	@echo "  make test        Run Rust unit tests"
	@echo "  make sdk-test    Run SDK comprehensive tests"
	@echo "  make stress      Run stress tests"
	@echo "  make full-test   Full integration test"
	@echo ""
	@echo "Other:"
	@echo "  make dashboard   Open dashboard in browser"
	@echo "  make clean       Clean build artifacts"

.DEFAULT_GOAL := help
