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
