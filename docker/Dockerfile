# Helper image to build hostd.
FROM docker.io/library/golang:1.20 AS builder

WORKDIR /hostd

# Copy and build binary.
COPY . .
RUN CGO_ENABLED=1 go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w -linkmode external -extldflags "-static"'  ./cmd/hostd

# Build image that will be used to run hostd.
FROM docker.io/library/alpine:3
LABEL maintainer="The Sia Foundation <info@sia.tech>" \
      org.opencontainers.image.description.vendor="The Sia Foundation" \
      org.opencontainers.image.description="A hostd container - provide storage on the Sia network and earn Siacoin" \
      org.opencontainers.image.source="https://github.com/SiaFoundation/hostd" \
      org.opencontainers.image.licenses=MIT

ENV PUID=0
ENV PGID=0

ENV HOSTD_API_PASSWORD=
ENV HOSTD_WALLET_SEED=

# Copy binary and prepare data dir.
COPY --from=builder /hostd/bin/* /usr/bin/
VOLUME [ "/data" ]

# API port
EXPOSE 9980/tcp
# RPC port
EXPOSE 9981/tcp
# RHP2 port
EXPOSE 9982/tcp
# RHP3 port
EXPOSE 9983/tcp

USER ${PUID}:${PGID}

ENTRYPOINT [ "hostd", "--docker", "--dir", "/data", "--http", ":9980" ]