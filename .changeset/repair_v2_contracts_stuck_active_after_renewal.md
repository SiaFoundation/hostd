---
default: patch
---

# Repair v2 contracts stuck "active" after renewal.

Adds a database migration that corrects v2 contracts whose renewal landed in the same block as a revision and was therefore never recorded against the parent. Affected contracts are flipped from active to renewed, their resolution index is populated from the renewed contract's confirmation index, and contract count, sector, revenue and collateral metrics are recalculated. Existing deployments no longer need a chain rescan to clear the stuck rows reported in #912.
