---
default: minor
---

# Added consensus pruning

Adds a new experimental config option to enable consensus pruning. With Utreexo, it is no longer required to store every block to fully validate new blocks. This option limits the number of blocks the host will store in its consensus database reducing the size of the consensus database on disk. It is currently defaulted to off. We recommend no less than one day of blocks to ensure protection for deep reorgs (144 blocks on mainnet).