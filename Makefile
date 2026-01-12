# FlashQ Makefile

.PHONY: dev run server release test postgres up down logs clean dashboard stress restart stop

# Development
dev:
	cd server && cargo run

# Run server with HTTP API (in-memory)
server:
	cd server && HTTP=1 GRPC=1 cargo run --release

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
	cd server && DATABASE_URL=postgres://flashq:flashq@localhost:5432/flashq HTTP=1 cargo run --release

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
	docker build -t flashq .

# Full test: start postgres, run server, run tests
full-test: up
	@echo "Starting server with persistence..."
	cd server && DATABASE_URL=postgres://flashq:flashq@localhost:5432/flashq HTTP=1 cargo run --release &
	@sleep 5
	@echo "Running SDK tests..."
	cd sdk/typescript && bun run examples/comprehensive-test.ts

# Stop server
stop:
	@pkill -f flashq-server 2>/dev/null || echo "Server not running"
	@echo "Server stopped"

# Restart server (with persistence)
restart: stop
	@sleep 1
	@cd server && DATABASE_URL=postgres://flashq:flashq@localhost:5432/flashq HTTP=1 cargo run --release --bin flashq-server &
	@sleep 3
	@echo "Server restarted with PostgreSQL persistence"
	@echo "Dashboard: http://localhost:6790"

# Restart server (memory only)
restart-mem: stop
	@sleep 1
	@cd server && HTTP=1 cargo run --release --bin flashq-server &
	@sleep 3
	@echo "Server restarted (memory only)"
	@echo "Dashboard: http://localhost:6790"

# Help
help:
	@echo "FlashQ Commands:"
	@echo ""
	@echo "  Server:"
	@echo "    make server     - Run release server (HTTP + gRPC, in-memory)"
	@echo "    make persist    - Run with PostgreSQL persistence"
	@echo "    make dev        - Run server in dev mode"
	@echo "    make stop       - Stop running server"
	@echo "    make restart    - Restart server with PostgreSQL"
	@echo ""
	@echo "  Docker:"
	@echo "    make up         - Start PostgreSQL container"
	@echo "    make down       - Stop all containers"
	@echo "    make logs       - View container logs"
	@echo ""
	@echo "  Testing:"
	@echo "    make test       - Run Rust unit tests"
	@echo "    make sdk-test   - Run SDK comprehensive tests"
	@echo "    make stress     - Run stress tests"
	@echo ""
	@echo "  Other:"
	@echo "    make dashboard  - Open dashboard in browser"
	@echo "    make clean      - Clean build artifacts"
