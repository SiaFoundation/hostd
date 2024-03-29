FROM docker.io/library/golang:1.21 AS builder

WORKDIR /hostd

# get dependencies
COPY go.mod go.sum ./
RUN go mod download

# copy source
COPY . .
# codegen
RUN go generate ./...
# build
RUN CGO_ENABLED=1 go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w -linkmode external -extldflags "-static"'  ./cmd/hostd

FROM docker.io/library/alpine:3
LABEL maintainer="The Sia Foundation <info@sia.tech>" \
      org.opencontainers.image.description.vendor="The Sia Foundation" \
      org.opencontainers.image.description="A hostd container - provide storage on the Sia network and earn Siacoin" \
      org.opencontainers.image.source="https://github.com/SiaFoundation/hostd" \
      org.opencontainers.image.licenses=MIT

ENV PUID=0
ENV PGID=0

ENV HOSTD_API_PASSWORD=
ENV HOSTD_SEED=
ENV HOSTD_CONFIG_FILE=/data/hostd.yml

# copy binary and prepare data dir.
COPY --from=builder /hostd/bin/* /usr/bin/
VOLUME [ "/data" ]

# API port
EXPOSE 9980/tcp
# RPC port
EXPOSE 9981/tcp
# RHP2 port
EXPOSE 9982/tcp
# RHP3 TCP port
EXPOSE 9983/tcp
# RHP3 WebSocket port
EXPOSE 9984/tcp

USER ${PUID}:${PGID}

ENTRYPOINT [ "hostd", "--env", "--dir", "/data", "--http", ":9980" ]