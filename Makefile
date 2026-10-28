SHELL := /bin/bash
GO ?= go
IMAGE ?= fbs:dev
PG_DSN ?= postgres://fbs:fbs@localhost:5432/fbs?sslmode=disable
TEST_ENV = FBS_TEST_PG_DSN="$(PG_DSN)" FBS_TEST_REDIS_ADDR=localhost:6379 FBS_TEST_KAFKA_BROKERS=localhost:19092
COMPOSE = docker compose -f deploy/compose/docker-compose.yml

.PHONY: all build test lint vet generate golden fixtures infra-up infra-down test-integration e2e docker docker-prebuilt compose-up compose-down spike-report phase0 phase0-dryrun spike-bin clean

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

PHASE0_CONFIG ?= phase0/proxies.yaml
PHASE0_PAGES ?= phase0/pages.csv
PHASE0_TIERS ?= isp,residential
PHASE0_OUT ?= phase0/out
PHASE0_ARGS ?=

## Phase 0 against real Facebook (docs/runbooks/phase0-spike.md). Fill in
## phase0/proxies.yaml and phase0/pages.csv first. Resume with PHASE0_ARGS="--from run".
phase0:
	@test -f $(PHASE0_CONFIG) || { echo "missing $(PHASE0_CONFIG): cp phase0/proxies.example.yaml $(PHASE0_CONFIG), fill it in, chmod 600"; exit 2; }
	@test -f $(PHASE0_PAGES) || { echo "missing $(PHASE0_PAGES): cp phase0/pages.example.csv $(PHASE0_PAGES) and fill it in"; exit 2; }
	$(GO) run ./cmd/fbs-spike phase0 --config $(PHASE0_CONFIG) --pages $(PHASE0_PAGES) --tiers $(PHASE0_TIERS) --out-dir $(PHASE0_OUT) $(PHASE0_ARGS)

## Rehearse Phase 0 against a local fakefb (no proxies, no Facebook, about a minute).
phase0-dryrun:
	GO=$(GO) phase0/dryrun.sh

## Static fbs-spike binaries in bin/, to run Phase 0 on a machine without Go or Docker.
spike-bin:
	@for t in linux/amd64 linux/arm64 darwin/arm64 darwin/amd64; do \
	  os=$${t%/*}; arch=$${t#*/}; \
	  echo "bin/fbs-spike-$$os-$$arch"; \
	  CGO_ENABLED=0 GOOS=$$os GOARCH=$$arch $(GO) build -trimpath \
	    -ldflags="-s -w -X main.version=$$(git describe --always --dirty 2>/dev/null || echo dev)" \
	    -o bin/fbs-spike-$$os-$$arch ./cmd/fbs-spike || exit 1; \
	done

clean:
	rm -rf bin/
