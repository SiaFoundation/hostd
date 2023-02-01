# Helper image to build hostd.
FROM golang AS builder
LABEL maintainer="The Sia Foundation <info@sia.tech>"

WORKDIR /hostd

# Copy and build binary.
COPY . .
RUN CGO_ENABLED=1 go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-linkmode external -extldflags "-static"'  ./cmd/hostd

# Build image that will be used to run hostd.
FROM alpine
LABEL maintainer="The Sia Foundation <info@sia.tech>"

ENV PUID=0
ENV PGID=0

ENV HOSTD_API_PASSWORD=

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

ENTRYPOINT [ "hostd", "--dir", "/data", "--http", ":9980" ]