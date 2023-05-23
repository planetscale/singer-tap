COMMIT := $(shell git rev-parse --short=7 HEAD 2>/dev/null)
VERSION := "0.1.19"
DATE := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
NAME := "stitch-source"
DOCKER_BUILD_PLATFORM := "linux/amd64"
DOCKER_LINUX_BUILD_PLATFORM := "linux/arm64/v8"
ifeq ($(strip $(shell git status --porcelain 2>/dev/null)),)
  GIT_TREE_STATE=clean
else
  GIT_TREE_STATE=dirty
endif

BIN := bin
export GOPRIVATE := github.com/planetscale/*
export GOBIN := $(PWD)/$(BIN)

GO ?= go
GO_ENV ?= PS_LOG_LEVEL=debug PS_DEV_MODE=1 CGO_ENABLED=0
GO_RUN := env $(GO_ENV) $(GO) run

OS := $(shell uname)
PROTOC_VERSION=3.20.1
PROTOC_ARCH=x86_64
ifeq ($(OS),Linux)
	PROTOC_PLATFORM := linux
endif
ifeq ($(OS),Darwin)
	PROTOC_PLATFORM := osx
endif
FIVETRANSDK_PROTO_OUT := proto/fivetransdk

.PHONY: all
all: build test lint-fmt lint

.PHONY: bootstrap
bootstrap:
	@go install mvdan.cc/gofumpt@latest

.PHONY: test
test:
	@go test ./...

.PHONY: build
build:
	@go build ./...

.PHONY: fmt
fmt: bootstrap
	$(GOBIN)/gofumpt -w .

.PHONY: server
server:
	@go run ./cmd/server/main.go

.PHONY: lint
lint:
	@go install honnef.co/go/tools/cmd/staticcheck@latest
	@staticcheck ./...

.PHONY: lint-fmt
lint-fmt: fmt
	git diff --exit-code

.PHONY: build-image
build-image:
	@echo "==> Building docker image ${REPO}/${NAME}:$(VERSION)"
	@docker build --platform ${DOCKER_BUILD_PLATFORM} --build-arg VERSION=$(VERSION:v%=%)  --build-arg GH_TOKEN=${GH_TOKEN} --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t ${REPO}/${NAME}:$(VERSION) .
	@docker tag ${REPO}/${NAME}:$(VERSION) ${REPO}/${NAME}:latest

.PHONY: build-image-linux
build-image-linux:
	@echo "==> Building docker image ${REPO}/${NAME}-linux:$(VERSION)"
	@docker build --platform ${DOCKER_LINUX_BUILD_PLATFORM} --build-arg VERSION=$(VERSION:v%=%)  --build-arg GH_TOKEN=${GH_TOKEN} --build-arg COMMIT=$(COMMIT) --build-arg DATE=$(DATE) -t ${REPO}/${NAME}-linux:$(VERSION) .
	@docker tag ${REPO}/${NAME}-linux:$(VERSION) ${REPO}/${NAME}-linux:latest

.PHONY: push
push: build-image
	export REPO=$(REPO)
	@echo "==> Pushing docker image ${REPO}/${NAME}:$(VERSION)"
	@docker push ${REPO}/${NAME}:latest
	@docker push ${REPO}/${NAME}:$(VERSION)
	@echo "==> Your image is now available at $(REPO)/${NAME}:$(VERSION)"

.PHONY: clean
clean:
	@echo "==> Cleaning artifacts"
	@rm ${NAME}
