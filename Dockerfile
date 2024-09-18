FROM golang:1.23-bullseye AS builder

ARG TARGETARCH

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt update && \
    apt install -y -q --no-install-recommends \
    build-essential gcc musl-dev linux-headers-${TARGETARCH} && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY . .

# system and linux dependencies
RUN make deps-linux

RUN make build

FROM debian:stable-slim

COPY --from=builder /build/bin/cmd/sidecar /usr/local/bin/sidecar

ENTRYPOINT ["/usr/local/bin/sidecar"]
