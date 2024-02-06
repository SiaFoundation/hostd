# [![Sia](https://sia.tech/assets/banners/sia-banner-expanded-hostd.png)](http://sia.tech)

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

## Configuration

`hostd` can be configured in multiple ways. Some settings, like the wallet key,
can be configured via environment variables or stdin. Others, like the RHP
ports, can be configured via CLI flags. To simplify more complex configurations,
`hostd` also supports the use of a YAML configuration file for all settings.

### Default Ports
+ `9980` - UI and API
+ `9981` - Sia consensus
+ `9982` - RHP2
+ `9983` - RHP3

#### Testnet
The Zen testnet version of `hostd` changes the default ports:

+ `9880` - UI and API
+ `9881` - Sia consensus
+ `9882` - RHP2
+ `9883` - RHP3

### Environment Variables
+ `HOSTD_API_PASSWORD` - The password for the UI and API
+ `HOSTD_SEED` - The recovery phrase for the wallet
+ `HOSTD_LOG_FILE` - changes the location of the log file. If unset, the
  log file will be created in the data directory
+ `HOSTD_CONFIG_FILE` - changes the path of the optional config file. If unset,
  `hostd` will check for a config file in the current directory

#### Testnet
The Zen testnet version of `hostd` changes the environment variables:

+ `HOSTD_ZEN_SEED` - The recovery phrase for the wallet
+ `HOSTD_ZEN_API_PASSWORD` - The password for the UI and API
+ `HOSTD_ZEN_LOG_PATH` - changes the path of the log file `hostd.log`. If unset, the
  log file will be created in the data directory

### CLI Flags
```sh
-bootstrap
	bootstrap the gateway and consensus modules
-dir string
	directory to store hostd metadata (default ".")
-env
	disable stdin prompts for environment variables (default false)
-http string
	address to serve API on (default ":9980")
-log.level string
	log level (debug, info, warn, error) (default "info")
-name string
	a friendly name for the host, only used for display
-rpc string
	address to listen on for peer connections (default ":9981")
-rhp2 string
	address to listen on for RHP2 connections (default ":9982")
-rhp3.tcp string
	address to listen on for TCP RHP3 connections (default ":9983")
-rhp3.ws string
	address to listen on for WebSocket RHP3 connections (default ":9984")
```

### YAML
All environment variables and CLI flags can be set via a YAML config file. The
config file defaults to `hostd.yml` in the current directory, but can be changed
with the `HOSTD_CONFIG_FILE` environment variable. All fields are optional and
default to the same values as the CLI flags.

```yaml
directory: /etc/hostd
recoveryPhrase: indicate nature buzz route rude embody engage confirm aspect potato weapon bid
http:
  address: :9980
  password: sia is cool
consensus:
  gatewayAddress: :9981
  bootstrap: true
rhp2:
  address: :9982
rhp3:
  tcp: :9983
  websocket: :9984
log:
  level: info # global log level
  stdout:
    enabled: true # enable logging to stdout
    level: info # log level for console logger
    format: human # log format (human, json)
    enableANSI: true # enable ANSI color codes (disabled on Windows)
  file:
    enabled: true # enable logging to file
    level: info # log level for file logger
    path: /var/log/hostd/hostd.log # the path of the log file
    format: json # log format (human, json)
```

# Building

`hostd` uses SQLite for its persistence. A gcc toolchain is required to build `hostd`

```sh
go generate ./...
CGO_ENABLED=1 go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w'  ./cmd/hostd
```

## Testnet Builds

`hostd` can be built to run on the Zen testnet by adding the `testnet` build
tag.

```sh
go generate ./...
CGO_ENABLED=1 go build -o bin/ -tags='testnet netgo timetzdata' -trimpath -a -ldflags '-s -w'  ./cmd/hostd
```

# Docker Support

`hostd` includes a `Dockerfile` which can be used for building and running
hostd within a docker container. The image can also be pulled from `ghcr.io/siafoundation/hostd`.

1. Generate a wallet seed using `hostd seed`.
2. Create `hostd.yml` in the directory you want to store your `hostd` data. Replace the recovery phrase with the one you generated above. Replace the password with a secure password to unlock the UI.

```yml
recoveryPhrase: indicate nature buzz route rude embody engage confirm aspect potato weapon bid
http:
  password: sia is cool
```

3. Create your docker container using one of the examples below. Replace "./data" in the volume mount with the directory in which you want to store your Sia data. Replace "./storage" in the volume mount with the location of your mounted storage volumes

## Mainnet

```sh
docker run -d \
  --name hostd \
  -p 127.0.0.1:9980:9980 \
  -p 9981-9983:9981-9983 \
  -v ./data:/data \
  -v ./storage:/storage \
    ghcr.io/siafoundation/hostd:latest
```

### Docker Compose

```yml
version: "3.9"
services:
  host:
    image: ghcr.io/siafoundation/hostd:latest
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

```sh
docker run -d \
  --name hostd \
  -p 127.0.0.1:9880:9880 \
  -p 9881-9883:9881-9883 \
  -v ./data:/data \
  -v ./storage:/storage \
  -e HOSTD_ZEN_SEED="my wallet seed" \
  -e HOSTD_ZEN_API_PASSWORD=hostsarecool \
    ghcr.io/siafoundation/hostd:latest-testnet
```

### Docker Compose

```yml
version: "3.9"
services:
  host:
    image: ghcr.io/siafoundation/hostd:latest-testnet
    environment:
      - HOSTD_ZEN_SEED=my wallet seed
      - HOSTD_ZEN_API_PASSWORD=hostsarecool
    ports:
      - 127.0.0.1:9880:9880/tcp
      - 9881-9883:9881-9883/tcp
    volumes:
      - /data:/data
      - /storage:/storage
    restart: unless-stopped
```

## Building image

### Mainnet

```sh
docker build -t hostd:latest -f ./docker/Dockerfile .
```

### Testnet

```sh
docker build -t hostd:latest-testnet -f ./docker/Dockerfile.testnet .
```
