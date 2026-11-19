---
default: patch
---

# Improved concurrent database performance

Disabled SQLite memory statistics tracking, which serialized concurrent queries on a global mutex.
