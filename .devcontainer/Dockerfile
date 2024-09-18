FROM golang:1.23-bullseye

ARG TARGETARCH

RUN export DEBIAN_FRONTEND=noninteractive && \
    apt update && \
    apt install -y -q --no-install-recommends \
    build-essential gcc musl-dev linux-headers-${TARGETARCH} && \
    apt clean && \
    rm -rf /var/lib/apt/lists/*

RUN useradd --create-home -s /bin/bash gobuild
RUN usermod -a -G sudo gobuild
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

ARG PROJECT=go-sidecar
RUN mkdir -p /workspaces/${PROJECT}
WORKDIR /workspaces/${PROJECT}
COPY --chown=gobuild:gobuild . .

# system and linux dependencies
RUN make deps-linux
RUN chown -R gobuild:gobuild /go

# local dependencies
ENV USER=gobuild
ENV GOBIN=/go/bin
ENV PATH=$PATH:${GOBIN}
USER gobuild

RUN git config --global --add safe.directory /workspaces/${PROJECT}

RUN make yamlfmt
RUN make fmtcheck
RUN make vet
RUN make lint
RUN make test
