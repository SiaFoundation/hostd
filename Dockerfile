FROM golang:1.24 AS builder

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

FROM debian:bookworm-slim

LABEL maintainer="The Sia Foundation <info@sia.tech>" \
org.opencontainers.image.description.vendor="The Sia Foundation" \
org.opencontainers.image.description="A hostd container - provide storage on the Sia network and earn Siacoin" \
org.opencontainers.image.source="https://github.com/SiaFoundation/hostd" \
org.opencontainers.image.licenses=MIT

# copy binary and certificates
COPY --from=builder /hostd/bin/* /usr/bin/
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

ENV HOSTD_DATA_DIR=/data
ENV HOSTD_CONFIG_FILE=/data/hostd.yml

VOLUME [ "/data" ]
# API port
EXPOSE 9980/tcp
# RPC port
EXPOSE 9981/tcp
# RHP2 port
EXPOSE 9982/tcp
# RHP3 TCP port
EXPOSE 9983/tcp
# RHP4 TCP port
EXPOSE 9984/tcp

ENTRYPOINT [ "hostd", "--env", "--http", ":9980" ]
