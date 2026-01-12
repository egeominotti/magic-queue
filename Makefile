# MagicQueue Makefile

.PHONY: dev run release test postgres up down logs clean dashboard stress restart stop

# Development
dev:
	cd server && cargo run

# Run with HTTP API and Dashboard
run:
	cd server && HTTP=1 cargo run

# Release build with HTTP
release:
	cd server && HTTP=1 cargo run --release

# Run tests
test:
	cd server && cargo test

# Start PostgreSQL only
postgres:
	docker-compose up -d postgres

# Start everything (PostgreSQL + wait)
up:
	docker-compose up -d postgres
	@echo "Waiting for PostgreSQL..."
	@sleep 3
	@echo "PostgreSQL ready!"

# Stop all containers
down:
	docker-compose down

# View logs
logs:
	docker-compose logs -f

# Run with PostgreSQL persistence
persist:
	docker-compose up -d postgres
	@sleep 3
	cd server && DATABASE_URL=postgres://magicqueue:magicqueue@localhost:5432/magicqueue HTTP=1 cargo run --release

# Open dashboard in browser (macOS)
dashboard:
	open http://localhost:6790

# Run SDK comprehensive tests
sdk-test:
	cd sdk/typescript && bun run examples/comprehensive-test.ts

# Run stress tests
stress:
	cd sdk/typescript && bun run examples/stress-test.ts

# Clean build artifacts
clean:
	cd server && cargo clean
	docker-compose down -v

# Build Docker image
docker-build:
	docker build -t magic-queue .

# Full test: start postgres, run server, run tests
full-test: up
	@echo "Starting server with persistence..."
	cd server && DATABASE_URL=postgres://magicqueue:magicqueue@localhost:5432/magicqueue HTTP=1 cargo run --release &
	@sleep 5
	@echo "Running SDK tests..."
	cd sdk/typescript && bun run examples/comprehensive-test.ts

# Stop server
stop:
	@pkill -f magic-queue-server 2>/dev/null || echo "Server not running"
	@echo "Server stopped"

# Restart server (with persistence)
restart: stop
	@sleep 1
	@cd server && DATABASE_URL=postgres://magicqueue:magicqueue@localhost:5432/magicqueue HTTP=1 cargo run --release --bin magic-queue-server &
	@sleep 3
	@echo "Server restarted with PostgreSQL persistence"
	@echo "Dashboard: http://localhost:6790"

# Restart server (memory only)
restart-mem: stop
	@sleep 1
	@cd server && HTTP=1 cargo run --release --bin magic-queue-server &
	@sleep 3
	@echo "Server restarted (memory only)"
	@echo "Dashboard: http://localhost:6790"

# Help
help:
	@echo "MagicQueue Commands:"
	@echo "  make dev        - Run server in dev mode"
	@echo "  make run        - Run with HTTP API"
	@echo "  make release    - Run release build with HTTP"
	@echo "  make persist    - Run with PostgreSQL persistence"
	@echo "  make test       - Run Rust tests"
	@echo "  make sdk-test   - Run SDK tests"
	@echo "  make stress     - Run stress tests"
	@echo "  make postgres   - Start PostgreSQL container"
	@echo "  make up         - Start PostgreSQL and wait"
	@echo "  make down       - Stop containers"
	@echo "  make logs       - View container logs"
	@echo "  make dashboard  - Open dashboard in browser"
	@echo "  make clean      - Clean build artifacts"
