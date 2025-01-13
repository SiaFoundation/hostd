---
default: major
---

# Renamed Environment Variables

Some of the environment variable names have been renamed for better consistency between `renterd`, `walletd`, and `hostd`. If your install was using environment variables for configuration, they may need to be updated. The preferred way to configure `hostd` is a YAML config file.

- `HOSTD_SEED` -> `HOSTD_WALLET_SEED`
- `HOSTD_LOG_FILE` -> `HOSTD_LOG_FILE_PATH`