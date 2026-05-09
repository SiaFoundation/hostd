---
default: patch
---

# Apply syncer rate limits to inbound peer connections.

The `syncerIngressLimit` and `syncerEgressLimit` settings now correctly throttle inbound peer connections in addition to outbound ones.
