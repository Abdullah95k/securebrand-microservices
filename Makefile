SHELL := /bin/bash
GO ?= go
IMAGE ?= fbs:dev
PG_DSN ?= postgres://fbs:fbs@localhost:5432/fbs?sslmode=disable
TEST_ENV = FBS_TEST_PG_DSN="$(PG_DSN)" FBS_TEST_REDIS_ADDR=localhost:6379 FBS_TEST_KAFKA_BROKERS=localhost:19092
COMPOSE = docker compose -f deploy/compose/docker-compose.yml

.PHONY: all build test lint vet generate golden fixtures infra-up infra-down test-integration e2e docker docker-prebuilt compose-up compose-down spike-report clean

all: generate build test

build:
	$(GO) build ./...

## Unit tests (no infrastructure). Integration tests skip themselves.
test:
	$(GO) test ./...

vet:
	$(GO) vet ./...

lint: vet
	@command -v golangci-lint >/dev/null && golangci-lint run ./... || echo "golangci-lint not installed; ran go vet only"
	@command -v buf >/dev/null && buf lint || echo "buf not installed; skipped proto lint"

## Regenerate protobuf code (requires buf + protoc-gen-go).
generate:
	buf generate

## Regenerate parser golden files after an intended parser change.
golden:
	$(GO) test ./internal/fb/parse -run TestGolden -update

## Regenerate the synthetic fakefb fixtures.
fixtures:
	$(GO) test -tags fixturegen ./internal/fakefb -run TestWriteFixtures

## Start Postgres, Redpanda and Valkey for integration tests.
infra-up:
	$(COMPOSE) up -d postgres redpanda valkey
	@until docker exec fbs-postgres-1 pg_isready -U fbs >/dev/null 2>&1; do sleep 1; done

infra-down:
	$(COMPOSE) down -v

## Integration tests against real Postgres/Redpanda/Valkey (make infra-up first).
test-integration:
	$(TEST_ENV) $(GO) test -count=1 ./internal/persist/... ./internal/e2e/... ./internal/spike/...

## Full pipeline end-to-end test only.
e2e:
	$(TEST_ENV) $(GO) test -count=1 -v ./internal/e2e/...

docker:
	docker build -f deploy/docker/Dockerfile -t $(IMAGE) .

## Build binaries on the host and package them (no Go builder image needed).
docker-prebuilt:
	CGO_ENABLED=0 GOOS=linux $(GO) build -trimpath -ldflags="-s -w" -o bin/ ./cmd/...
	docker build -f deploy/docker/Dockerfile.prebuilt -t $(IMAGE) .

compose-up:
	$(COMPOSE) up -d

compose-down:
	$(COMPOSE) down -v

spike-report:
	$(GO) run ./cmd/fbs-spike report --in spike-results.jsonl

clean:
	rm -rf bin/
