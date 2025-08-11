---
default: patch
---

# Fixed rejected renewal contract failures

Renewed contracts that do not make it on chain will no longer fail and adds a 6 block buffer
before storage is reclaimed to ensure small reorgs do not cause unnecessary contract failures.