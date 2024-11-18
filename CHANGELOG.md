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
