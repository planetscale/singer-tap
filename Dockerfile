# syntax=docker/dockerfile:1

ARG GO_VERSION=1.21.3
FROM pscale.dev/wolfi-prod/go:${GO_VERSION} AS build

WORKDIR /singer-tap
COPY . .

RUN go mod download
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath ./cmd/singer-tap

FROM pscale.dev/wolfi-prod/base:latest

COPY --from=build /singer-tap /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/singer-tap"]
