# [![Sia](https://sia.tech/banners/sia-banner-hostd.png)](http://sia.tech)

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

## Current Status
Please be advised that `hostd` is currently in alpha and should only be used on
the Zen testnet. Using `hostd` on the main Sia network is not recommended. By
limiting its use to the Zen testnet, you can safely explore its features and
contribute to its improvement without risking your assets. 

## What's Next?
Our current goal is to stabilize `hostd`, implement the UI, and enable migration
for existing `siad` hosts. A project roadmap is available on
[GitHub](https://github.com/orgs/SiaFoundation/projects/3)

# Building
`hostd` uses SQLite for its persistence. A gcc toolchain is required to build `hostd`
```sh
CGO_ENABLED=1 go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-linkmode external -extldflags "-static"'  ./cmd/hostd
```

## Testnet Builds
`hostd` can be built to run on the Zen testnet by adding the `testnet` build tag.
```sh
CGO_ENABLED=1 go build -o bin/ -tags='testnet netgo timetzdata' -trimpath -a -ldflags '-linkmode external -extldflags "-static"'  ./cmd/hostd
```

# Docker
hostd has a Dockerfile for easy containerization. The image can be pulled from `ghcr.io/siafoundation/hostd`.

## Running container
```sh
docker run -d \
--name hostd \
-p 127.0.0.1:9980:9980 \
-p 9981-9983:9981-9983 \
-v /data:/data
-v /storage:/storage
-e HOSTD_API_PASSWORD=hostsarecool \
ghcr.io/siafoundation/hostd:latest
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

## Testnet
Suffix any tag with `-testnet` to use the testnet image.

## Building image
```sh
docker build -t hostd -f ./docker/Dockerfile .
```