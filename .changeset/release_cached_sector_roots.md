---
default: patch
---

# Release cached sector roots of resolved and rejected contracts

Fixes a memory leak where the in-memory sector root cache kept the roots of every resolved, renewed, and rejected contract for the lifetime of the process.
