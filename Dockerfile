FROM golang:1.24.0-bookworm AS builder

ARG TARGETARCH

WORKDIR /build

COPY . .

# system and linux dependencies
RUN make deps/go

RUN make build

FROM golang:1.24.0-bookworm

RUN apt-get update && apt-get install -y ca-certificates postgresql-client

COPY --from=builder /build/bin/sidecar /usr/local/bin/sidecar

ENTRYPOINT ["/usr/local/bin/sidecar"]
