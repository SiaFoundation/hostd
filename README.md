`hostd` is a next-generation Sia host, developed by the Sia Foundation. 

# Building
`hostd` uses SQLite for its persistence. A gcc toolchain is required to build `hostd`
```sh
CGO_ENABLED=1 go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-linkmode external -extldflags "-static"'  ./cmd/hostd
```

# Docker
hostd has a Dockerfile for easy containerization. The image can be pulled from `ghcr.io/siafoundation/hostd`.

## Building image
```sh
docker build -f ./docker/Dockerfile .
```

## Running container
```sh
docker run -d \
--name hostd \
-p 127.0.0.1:9980:9980 \
-p 9981-9983:9981-9983 \
-v /data:/data
-v /storage:/storage
-e HOSTD_API_PASSWORD=hostsarecool \
```

## Docker Compose
```yml
version: "3.9"
services:
  host:
    image: ghcr.io/siafoundation/hostd:latest
    environment:
      - HOSTD_API_PASSWORD=hostsarecool
    ports:
      - "9980:9980/tcp"
      - "9981:9981/tcp"
      - "9982:9982/tcp"
      - "9983:9983/tcp"
    volumes:
      - "/data:/data"
      - "/storage:/storage"
    restart: unless-stopped
```