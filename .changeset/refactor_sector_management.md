---
default: minor
---

# Refactor Sector Management

Improves sector lookups by 50% on average by removing the sector lock tables and moving reference pruning out of the hot path.