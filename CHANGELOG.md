## 2.5.0 (2025-10-23)

### Features

- Trigger rescan when wallet seed changes

### Fixes

- Add OpenAPI spec
- Archive V1 contracts with proof window after require height.
- Fix integrity checks using cache
- Fixed RPCFreeSectors NDFs.
- Update coreutils to v0.18.5

## 2.4.1 (2025-09-05)

### Fixes

- Update core dependency from 0.17.4 to 0.17.5 and coreutils dependency from 0.18.2 to 0.18.4.

## 2.4.0 (2025-08-27)

### Features

- Added `[GET] /wallet/events/:id`
- Contracts that have been rejected can now be retried by renters.
- Remove RHP2 and RHP3 support
- Removed V1 support

### Fixes

- Added alert for expiring local certificates
- Added a 6 block buffer before storage is reclaimed to ensure small reorgs do not cause unnecessary contract failures.
- Fix integrity checks for v2 contracts
- Fixed connectivity check overflow causing immediate retries after many failures.
- Fixed an issue where renewed contracts that are rejected will cause the original contract to fail.
- Updated coreutils to v0.18.2 and core to v0.17.4

## 2.3.7 (2025-08-10)

### Fixes

- Fixed a race condition with Merkle proofs when broadcasting storage proofs.
- Fixed panic when attempting to broadcast v2 contract revision.

## 2.3.6 (2025-08-01)

### Fixes

- Fixed a panic when a v2 contract state transitions from renewed to failed.

## 2.3.5 (2025-07-01)

### Fixes

- Fix SQL error when fetching accounts.
- Prevent volume writes during shutdown.
- Reintroduce db transaction retries for 'database is locked' errors

## 2.3.4 (2025-06-25)

### Fixes

- Don't log error when testing connection fails on shutdown
- Remove unused Explorer interface
- Reset chain state when bootstrapping.

## 2.3.3 (2025-06-20)

### Fixes

- Prevent new v1 contracts from being formed post hardfork activation.
- Prevent revising rejected contracts.

## 2.3.2 (2025-06-17)

### Fixes

- Fix error reverting successful v1 contracts
- Fixed an issue reverting renewed v2 contracts.
- Add default QUIC listener if not specified in config.
- Update coreutils to v0.16.2 and core to v0.13.2

## 2.3.1 (2025-06-12)

### Fixes

- Fixed a panic when the explorer is disabled.
- Fixed an issue with nomad reissuing certificates on startup.
- Return error message from forex API

## 2.3.0 (2025-06-07)

### Features

#### Add `nomad` Certificates

Adds `nomad` as an experimental zero-config TLS Certificate provider for Sia hosts. `nomad` is run by the Sia Foundation to reduce friction of setting up a storage provider that supports WebTransport in browsers.

#### Add `nomad` automatic certificate generation

Optionally uses the Sia Foundation's `nomad` service to generate a `*.sia.host` TLS certificate to support the WebTransport protocol in browsers.

### Fixes

- Added fallback support for wildcard certificates.
- Fixed a panic when updating contract formation proofs.
- Fixed an issue with concurrent map access with webhook broadcasts.
- Updated coreutils to v0.16.1

## 2.2.3 (2025-05-29)

### Fixes

- Include QUIC in host announcement if configured.
- Update core to v0.13.1 and coreutils to v0.15.2
- Use WindowEnd instead of WindowStart when checking if the renter attempts to form a contract too close to the v2 require height (RHP2).

## 2.2.2 (2025-05-28)

### Fixes

- Use WindowEnd instead of WindowStart when checking if the renter attempts to form a contract too close to the v2 require height.

## 2.2.1 (2025-05-27)

### Fixes

- Fixed version update spam.

## 2.2.0 (2025-05-26)

### Features

- Added `[PUT] /api/system/connect/test` to trigger a test of the host's connectivity.
- Added periodic version checks.

#### Added periodic connection tests

Periodically tests the hosts connection using the SiaScan troubleshooting API. This provides alerts to hosts if for some reason their node is not connectable. This system relies on a central server for the test and may return occasional false positives. The server can be overridden by setting `Config.Explorer.URL`

#### Simplified V1 contract sectors management

Changes sector updates for V1 contracts to match the behavior from V2. The new behavior diffs the old and new sector roots and updates the changed indices in the database rather than relying on replaying the sector changes. This is slightly slower on large contracts due to needing to calculate the diff of each index, but it is significantly simpler and less prone to edge cases.

### Fixes

- Added volume and contract metrics to recalc command
- Fixed a panic in RHP2 sector roots RPC.
- Fixed consensus v2 commitment.
- Fixed a panic when encoding the alerts array during high frequency updates
- Locked wallet UTXOs will now be stored in the database.
- Updated core to v0.13.0 and coreutils to v0.15.0

## 2.1.0 (2025-04-29)

### Features

- The RHP4 port can now be set using the `-rhp4` flag.
- Added Prometheus support to `[GET] /syncer/peers`

#### Add endpoints for querying V2 contracts

Adds two new endpoints for querying V2 contracts

#### `[GET] /v2/contracts/:id`

Returns a single V2 contract by its ID

#### `[POST] /v2/contracts`

Queries a list of contracts with optional filtering

**Example request  body**
```json
{
  "statuses": [],
  "contractIDs": [],
  "renewedFrom": [],
  "renewedTo": [],
  "renterKey": [],
  "minNegotiationHeight": 0,
  "maxNegotiationHeight": 0,
  "minExpirationHeight": 0,
  "maxExpirationHeight": 0,
  "limit": 0,
  "offset": 0,
  "sortField": "",
  "sortDesc": false
}
```

#### Add QUIC support

Adds QUIC transport support in RHP4, enabling Sia network access through web browsers. This update also simplifies cross-language development by using a standard protocol.

#### Add support for RPCReplenish

Adds support RPCReplenish, simplifying management for renters with a large number of accounts.

### Fixes

- Combine update and select query when pruning sectors to force usage of exclusive database transaction
- Fixed broken API client consensus methods
- Fix JSON field names for V2Contract type
- Fixed an issue with v2 contract sector roots not being reloaded
- Limit the number of parallel SQLite connections to 1 to prevent "database is locked" errors.
- Reduced migration time for large mostly empty volumes
- Update jape dependency to v0.13.0.
- Update module path to go.sia.tech/hostd/v2

## 2.0.4 (2025-02-24)

### Fixes

- Fix decoding error on RHP3 account id type
- Update core and coreutils dependencies to v0.10.1 and v0.11.1 respectively

## 2.0.3 (2025-02-07)

### Fixes

- Fix a potential panic when rpcLock fails
- Fixed a concurrency issue when migrating data from large volumes

#### Fixed an inconsistency migrating the config's log directory from the v1.1.2 config file

The deprecated `Log.Path` config field was a directory, while the new `Log.File.Path` is expected to be a file path. If the log directory was set, the log file path will be correctly migrated to `hostd.log` in the directory.

## 2.0.2 (2025-01-16)

### Fixes

- Add missing database index for migrated nodes
- Added "sqlite integrity" command
- Fixed an issue with contracts sometimes being rejected if one of their parent transactions was confirmed in an earlier block

## 2.0.1 (2025-01-13)

### Fixes

- Fixed an issue with alert content not showing in the UI

## 2.0.0 (2025-01-13)

### Breaking Changes

- Remove RHP Session endpoints
- Switched Docker base image from scratch to debian:bookworm-slim

#### Combined Mainnet and Testnet Binaries

This changes `hostd` to support multiple networks from a single binary. Instead of needing to download the Zen specific binary, users can switch networks through the YAML config or a CLI flag: `--network=zen`.

This does have some broader implications. The environment variables and default ports have also been combined. Users running both Mainnet and Zen nodes will need to manually change ports in their config files.

##### Removed Environment Variables
- `HOSTD_ZEN_SEED` -> `HOSTD_WALLET_SEED`
- `HOSTD_ZEN_API_PASSWORD` -> `HOSTD_API_PASSWORD`
- `HOSTD_ZEN_LOG_PATH` -> `HOSTD_LOG_FILE_PATH`

#### Renamed Environment Variables

Some of the environment variable names have been renamed for better consistency between `renterd`, `walletd`, and `hostd`. If your install was using environment variables for configuration, they may need to be updated. The preferred way to configure `hostd` is a YAML config file.

- `HOSTD_SEED` -> `HOSTD_WALLET_SEED`
- `HOSTD_LOG_FILE` -> `HOSTD_LOG_FILE_PATH`

#### Support V2 Hardfork

The V2 hardfork is scheduled to modernize Sia's consensus protocol, which has been untouched since Sia's mainnet launch back in 2014, and improve accessibility of the storage network. To ensure a smooth transition from V1, it will be executed in two phases. Additional documentation on upgrading will be released in the near future.

##### V2 Highlights
- Drastically reduces blockchain size on disk
- Improves UTXO spend policies - including HTLC support for Atomic Swaps
- More efficient contract renewals - reducing lock up requirements for hosts and renters
- Improved transfer speeds - enables hot storage

##### Phase 1 - Allow Height
- **Activation Height:** `513400` (March 10th, 2025)
- **New Features:** V2 transactions, contracts, and RHP4
- **V1 Support:** Both V1 and V2 will be supported during this phase
- **Purpose:** This period gives time for integrators to transition from V1 to V2
- **Requirements:** Users will need to update to support the hardfork before this block height

##### Phase 2 - Require Height
- **Activation Height:** `526000` (June 6th, 2025)
- **New Features:** The consensus database can be trimmed to only store the Merkle proofs
- **V1 Support:** V1 will be disabled, including RHP2 and RHP3. Only V2 transactions will be accepted
- **Requirements:** Developers will need to update their apps to support V2 transactions and RHP4 before this block height

#### Switch to `core` consensus

Removes the last legacy code from `siad`. Switching between Mainnet and Testnet can now be done with a CLI flag e.g. `--network=zen`. This will require hosts to resync the blockchain. Refrain from upgrading if contracts are about to expire.

#### Use standard locations for application data

Uses standard locations for application data instead of the current directory. This brings `hostd` in line with other system services and makes it easier to manage application data.

##### Linux, FreeBSD, OpenBSD
- Configuration: `/etc/hostd/hostd.yml`
- Data directory: `/var/lib/hostd`

##### macOS
- Configuration: `~/Library/Application Support/hostd.yml`
- Data directory: `~/Library/Application Support/hostd`

##### Windows
- Configuration: `%APPDATA%\SiaFoundation\hostd.yml`
- Data directory: `%APPDATA%\SiaFoundation\hostd`

##### Docker
- Configuration: `/data/hostd.yml`
- Data directory: `/data`

### Features

- Attempt to upgrade existing configs instead of exiting

#### Add RHP4 support

RHP4 is the next generation of renter-host protocol for the Sia storage network. It dramatically increases performance compared to RHP3, improves contract payouts, reduces collateral lockup, and enables features necessary for light clients. The new protocol will be available and activated starting at block height `513400` (March 10th, 2025).

#### Refactor Sector Management

Improves sector lookups by 50% on average by removing the sector lock tables and moving reference pruning out of the hot path.

### Fixes

- Automate changelog generation
- Fixed a major speed regression when uploading data on larger nodes
- Fixed a panic updating metrics when a contract formation is reverted
- Fixed an issue with price pinning not updating prices
- Move RHP2 and RHP3 settings into the config manager to be consistent with RHP4
- The default number of registry entries is now 0. The registry is deprecated and will not be supported after the V2 hardfork.

## 1.1.2

### Breaking changes
* Max collateral no longer auto-calculates and is visible in "Basic" pricing mode (https://github.com/SiaFoundation/hostd/pull/421)

### Features
* Migrated sectors are included in volume writes (https://github.com/SiaFoundation/hostd/pull/395)
* Timeout was increased when writing large merkle proofs for slow connections (https://github.com/SiaFoundation/hostd/pull/408)

### Fixed
* Fixed an error when renters attempted to trim a large number of sectors from a contract https://github.com/SiaFoundation/hostd/pull/410
* Fixed the calculation for unconfirmed transactions (https://github.com/SiaFoundation/hostd/pull/415)
* Fixed an issue where transaction information would not show when clicking a transaction in the wallet (https://github.com/SiaFoundation/hostd/pull/416)
* `hostd` will no longer crash on startup if the explorer is unavailable or misconfigured (https://github.com/SiaFoundation/hostd/pull/430)
