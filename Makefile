# Notification service — common developer commands.

.PHONY: up down test lint run deploy smoke

## up: start the full stack (postgres, rabbitmq, api, worker)
up:
	docker compose up -d --build

## down: stop the stack (add -v manually to wipe volumes)
down:
	docker compose down

## test: unit tests with the race detector (requires cgo; CI enforces this)
test:
	go test -race ./...

## lint: formatting, vet, and golangci-lint when installed
lint:
	gofmt -l .
	go vet ./...
	@golangci-lint run ./... 2>/dev/null || echo "golangci-lint not installed; skipped (CI runs it)"

## run: run api+worker in one process against a running compose stack
run:
	go run ./cmd/notifier

## deploy: build the image and roll the local stack
deploy:
	./scripts/deploy.sh local

## smoke: full test matrix including live e2e checks
smoke:
	./scripts/test.sh all
