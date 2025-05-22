---
default: minor
---

# Simplified V1 contract sectors management

Changes sector updates for V1 contracts to match the behavior from V2. The new behavior diffs the old and new sector roots and updates the changed indices in the database rather than relying on replaying the sector changes. This is slightly slower on large contracts due to needing to calculate the diff of each index, but it is significantly simpler and less prone to edge cases.