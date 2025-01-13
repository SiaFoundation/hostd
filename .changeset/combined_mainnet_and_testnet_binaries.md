---
default: major
---

# Combined Mainnet and Testnet Binaries

This changes `hostd` to support multiple networks from a single binary. Instead of needing to download the Zen specific binary, users can switch networks through the YAML config or a CLI flag: `--network=zen`.

This does have some broader implications. The environment variables and default ports have also been combined. Users running both Mainnet and Zen nodes will need to manually change ports in their config files.

#### Removed Environment Variables
- `HOSTD_ZEN_SEED` -> `HOSTD_WALLET_SEED`
- `HOSTD_ZEN_API_PASSWORD` -> `HOSTD_API_PASSWORD`
- `HOSTD_ZEN_LOG_PATH` -> `HOSTD_LOG_FILE_PATH`