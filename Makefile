# Notification service — common developer commands.

.PHONY: up down test lint run deploy smoke

# The race detector needs cgo (a C toolchain). Detect instead of assume,
# so `make test` works on machines without gcc; CI always runs -race.
RACEFLAG := $(if $(filter 1,$(shell go env CGO_ENABLED)),-race,)

## up: start the full stack (postgres, rabbitmq, api, worker)
up:
	docker compose up -d --build

## down: stop the stack (add -v manually to wipe volumes)
down:
	docker compose down

## test: unit tests, with the race detector when cgo is available
test:
	$(if $(RACEFLAG),,@echo "note: cgo unavailable, running without -race (CI runs it)")
	go test $(RACEFLAG) ./...

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
