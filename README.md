# [![Sia](https://sia.tech/banners/sia-banner-expanded-hostd.png)](http://sia.tech)

[![GoDoc](https://godoc.org/go.sia.tech/hostd?status.svg)](https://godoc.org/go.sia.tech/hostd)

A host for Sia.

## Overview

`hostd` is an advanced Sia host solution created by the Sia Foundation, designed
to enhance the experience for storage providers within the Sia network. Tailored
for both individual and large-scale storage providers, `hostd` boasts a
user-friendly interface and a robust API, empowering providers to efficiently
manage their storage resources and revenue. `hostd` incorporates an embedded
web-UI, simplifying deployment and enabling remote management capabilities,
ensuring a smooth user experience across a diverse range of devices.

- A project roadmap is available on [GitHub](https://github.com/orgs/SiaFoundation/projects/3)
- Setup guides are available at [https://docs.sia.tech](https://docs.sia.tech/hosting/hostd/about-hosting-on-sia)

### Ports
`hostd` uses the following ports:
+ `9980` - UI and API
+ `9981` - Sia consensus
+ `9982` - RHP2
+ `9983` - RHP3

### Environment Variables
`hostd` supports the following environment variables:
+ `HOSTD_API_PASSWORD` - The password for the UI and API
+ `HOSTD_SEED` - The recovery phrase for the wallet
+ `HOSTD_LOG_PATH` - changes the path of the log file `hostd.log`. If unset, the
  log file will be created in the data directory

# Building

`hostd` uses SQLite for its persistence. A gcc toolchain is required to build `hostd`

```sh
go generate ./...
CGO_ENABLED=1 go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w'  ./cmd/hostd
```

## Testnet Builds

`hostd` can be built to run on the Zen testnet by adding the `testnet` build
tag.

The Zen testnet version of `hostd` changes the environment variables and default
ports:
+ `HOSTD_ZEN_SEED` - The recovery phrase for the wallet
+ `HOSTD_ZEN_API_PASSWORD` - The password for the UI and API

+ `9880` - UI and API
+ `9881` - Sia consensus
+ `9882` - RHP2
+ `9883` - RHP3

```sh
go generate ./...
CGO_ENABLED=1 go build -o bin/ -tags='testnet netgo timetzdata' -trimpath -a -ldflags '-s -w'  ./cmd/hostd
```

# Docker

`hostd` has a Dockerfile for easy containerization. The image can be pulled from `ghcr.io/siafoundation/hostd`.

## Running container

```sh
docker run -d \
  --name hostd \
  -p 127.0.0.1:9980:9980 \
  -p 9981-9983:9981-9983 \
  -v ./data:/data \
  -v ./storage:/storage \
  -e HOSTD_SEED="my wallet seed" \
  -e HOSTD_API_PASSWORD=hostsarecool \
    ghcr.io/siafoundation/hostd:latest
```

### Docker Compose

```yml
version: "3.9"
services:
  host:
    image: ghcr.io/siafoundation/hostd:latest
    environment:
      - HOSTD_SEED=my wallet seed
      - HOSTD_API_PASSWORD=hostsarecool
    ports:
      - 127.0.0.1:9980:9980/tcp
      - 9981-9983:9981-9983/tcp
    volumes:
      - /data:/data
      - /storage:/storage
    restart: unless-stopped
```

## Testnet

Suffix any tag with `-testnet` to use the testnet image.

The Zen testnet version of `hostd` changes the environment variables and default
ports:
+ `HOSTD_ZEN_SEED` - The recovery phrase for the wallet
+ `HOSTD_ZEN_API_PASSWORD` - The password for the UI and API
+ `HOSTD_ZEN_LOG_PATH` - changes the path of the log file `hostd.log`. If unset, the
  log file will be created in the data directory

+ `9880` - UI and API
+ `9881` - Sia consensus
+ `9882` - RHP2
+ `9883` - RHP3

## Building image

### Production

```sh
docker build -t hostd:latest -f ./docker/Dockerfile .
```

### Testnet

```sh
docker build -t hostd:latest-testnet -f ./docker/Dockerfile.testnet .
```
