# syntax=docker/dockerfile:1

ARG GO_VERSION=1.18rc1
FROM golang:${GO_VERSION}-bullseye AS build

WORKDIR /singer-tap
COPY . .

RUN go mod download
RUN go build ./cmd/singer-tap

FROM debian:bullseye-slim

RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y default-mysql-client ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=build /singer-tap /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/singer-tap"]
