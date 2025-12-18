---
default: minor
---

# Add support for instant sync

New users can sync instantly using `hostd --instant`. When instant syncing, the `hostd` node initializes using a Utreexo-based checkpoint and can immediately validate blocks from that point forward without replaying the whole chain state. The state is extremely compact and committed in block headers, making this initialization both quick and secure. [Learn more](https://sia.tech/learn/instant-syncing)

**The wallet is required to only have v2 history to use instant syncing.**
