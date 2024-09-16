# Use the standard go 1.22 alpine image as our build base
FROM golang:1.22-alpine AS build

# Install needed dependencies to build
RUN apk add --no-cache gcc musl-dev linux-headers make

COPY . /build
WORKDIR /build

RUN CGO_ENABLED=1 make build

# Pull compiled binaries into a vanilla alpine base image
FROM alpine:latest

RUN apk add --no-cache ca-certificates

COPY --from=build /build /go-sidecar

ENTRYPOINT ["/go-sidecar/bin/cmd/sidecar"]
