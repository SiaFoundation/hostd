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

### RHP4

RHP4 is the latest version of the renter–host protocol, delivering major performance improvements over RHP2 and RHP3. It supports 
significantly higher throughput and concurrency, enabling hosts to handle more renters in parallel with lower latency. 

For maximum interoperability, RHP4 supports both:
- **SiaMux (TCP)** — Secure, high-speed multiplexing layer, built by the Sia Foundation, allowing multiple protocol streams over a single connection. Reliable connections for most environments.
- **QUIC (UDP)** — Modern, secure, multiplexed connections ideal for direct browser access. QUIC requires no special setup, but for better decentralization, configure a custom domain with a valid TLS certificate (`certPath` and `keyPath` in config).

## Configuration

The YAML config file is the recommended way to configure `hostd`. `hostd` includes a command to interactively generate a config file: `hostd config`. Some settings can be overridden using CLI flags or environment variables. 

### Default Paths

#### Data Directory

The host's consensus database, host database, and log files are stored in the data directory.

Operating System | Path
---|---
Windows | `%APPDATA%/hostd`
macOS | `~/Library/Application Support/hostd`
Linux | `/var/lib/hostd`
Docker | `/data`

#### Config File

Operating System | Path
---|---
Windows | `%APPDATA%/hostd/hostd.yml`
macOS | `~/Library/Application Support/hostd/hostd.yml`
Linux | `/etc/hostd/hostd.yml`
Docker | `/data/hostd.yml`

The default config path can be changed using the `HOSTD_CONFIG_FILE` environment variable. For backwards compatibility with earlier versions, `hostd` will also check for `hostd.yml` in the current directory.

### Default Ports

+ `9980/TCP` - UI and API
+ `9981/TCP` - Sia consensus
+ `9984/TCP` - RHP4 (SiaMux)
+ `9984/UDP` - RHP4 (QUIC)

*When running in Docker, bind `9980` to `127.0.0.1` unless you explicitly intend to expose the API publicly.*

### Example Config File

```yaml
directory: /etc/hostd
recoveryPhrase: indicate nature buzz route rude embody engage confirm aspect potato weapon bid
http:
  address: :9980
  password: sia is cool
syncer:
  address: :9981
  bootstrap: true
consensus:
  network: mainnet
  indexBatchSize: 100
rhp2:
  address: :9982
rhp3:
  tcp: :9983
rhp4:
  listenAddresses:
    - protocol: tcp # tcp,tcp4 or tcp6
      address: :9984
    - protocol: quic # quic, quic4, quic6
      address: :9984
  quic:
    certPath: '/path/certs/certchain.crt' # Certificate chain file (optional)
    keyPath: '/path/private/keyfile.key' # Certificate private keyfile (optional)
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

### Environment Variables
+ `HOSTD_API_PASSWORD` - The password for the UI and API
+ `HOSTD_WALLET_SEED` - The recovery phrase for the wallet
+ `HOSTD_LOG_FILE` - changes the location of the log file. If unset, the log file will be created in the data directory
+ `HOSTD_CONFIG_FILE` - changes the path of the `hostd` config file.

### CLI Flags
```sh
-dir string
	directory to store hostd metadata defaults to the current directory
-http string
	address to serve API on (default "localhost:9980")
-log.level string
	log level (debug, info, warn, error) (default "debug")
-name string
	a friendly name for the host, only used for display
-network string
	network name (mainnet, testnet, etc) (default "mainnet")
-openui
	automatically open the web UI on startup (default true)
-syncer.address string
	address to listen on for peer connections (default ":9981")
-syncer.bootstrap
	bootstrap the gateway and consensus modules (default true)
-rhp2 string
	address to listen on for RHP2 connections (default ":9982")
-rhp3 string
	address to listen on for TCP RHP3 connections (default ":9983")
-rhp4 string
        address to listen on for RHP4 connections
-env
	disable stdin prompts for environment variables (default false)
```

# Building

`hostd` uses SQLite for its persistence. A gcc toolchain is required to build `hostd`

```sh
go generate ./...
CGO_ENABLED=1 go build -o bin/ -tags='netgo timetzdata' -trimpath -a -ldflags '-s -w'  ./cmd/hostd
```

# Docker

`hostd` includes a `Dockerfile` which can be used for building and running
hostd within a docker container. The image can also be pulled from `ghcr.io/siafoundation/hostd:latest`.

Be careful with port `9980` as Docker will expose it publicly by default. It is
recommended to bind it to `127.0.0.1` to prevent unauthorized access.

## Creating the container

### 1. Create the compose file 

Create a new file named `docker-compose.yml`. You can use the following as a template. The `/data` mount is where consensus data is stored and is required. Change the `/storage` volume to the path of your storage drive. If you have additional mount points, add them.

```yml
services:
  host:
    image: ghcr.io/siafoundation/hostd:latest
    ports:
      - 127.0.0.1:9980:9980/tcp
      - 9981-9984:9981-9984/tcp
      - 9984:9984/udp
    volumes:
      - hostd-data:/data
      - /storage:/storage
    restart: unless-stopped

volumes:
  hostd-data:
```

### 2. Configure `hostd`

Run the following command to generate a config file for your host. **Do not change the data directory from `/data`.**
```
docker compose run -it host config
```

### 3. Start `hostd`

After creating a config file, it's time to start your host:
```
docker compose up -d
```

## Building Image

```sh
docker build -t hostd .
```
