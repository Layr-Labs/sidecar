FROM golang:1.23.6-bookworm AS builder

ARG TARGETARCH

WORKDIR /build

COPY . .

# system and linux dependencies
RUN make deps/go

RUN make build

FROM golang:1.23.6-bookworm

RUN apt-get update && apt-get install -y ca-certificates gnupg2

RUN wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -
RUN sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ bookworm-pgdg main" > /etc/apt/sources.list.d/pgdg.list'
RUN apt-get update && apt-get install -y postgresql-client-16

COPY --from=builder /build/bin/sidecar /usr/local/bin/sidecar

ENTRYPOINT ["/usr/local/bin/sidecar"]
