---
default: major
---

# Switch to `core` consensus

Removes the last legacy code from `siad`. Switching between Mainnet and Testnet can now be done with a CLI flag e.g. `--network=zen`. This will require hosts to resync the blockchain. Refrain from upgrading if contracts are about to expire.
