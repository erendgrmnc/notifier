# Notification service — common developer commands.

.PHONY: up down test lint run deploy smoke

# The race detector needs cgo (a C toolchain). Detect instead of assume,
# so `make test` works on machines without gcc; CI always runs -race.
RACEFLAG := $(if $(filter 1,$(shell go env CGO_ENABLED)),-race,)

# On native Windows make (no sh in PATH), dispatch to the .bat twins;
# elsewhere use the POSIX scripts.
ifeq ($(OS),Windows_NT)
DEPLOY_SCRIPT = scripts\deploy.bat
TEST_SCRIPT   = scripts\test.bat
NULL_DEVICE   = NUL
else
DEPLOY_SCRIPT = ./scripts/deploy.sh
TEST_SCRIPT   = ./scripts/test.sh
NULL_DEVICE   = /dev/null
endif

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

## test-verbose: every test name and result, plus per-package coverage
test-verbose:
	go test -v -cover $(RACEFLAG) ./...

## lint: formatting, vet, and golangci-lint when installed
lint:
	gofmt -l .
	go vet ./...
	@golangci-lint run ./... 2>$(NULL_DEVICE) || echo "golangci-lint not installed; skipped (CI runs it)"

## run: run api+worker in one process against a running compose stack
run:
	go run ./cmd/notifier

## deploy: build the image and roll the local stack
deploy:
	$(DEPLOY_SCRIPT) local

## smoke: full test matrix including live e2e checks
smoke:
	$(TEST_SCRIPT) all
