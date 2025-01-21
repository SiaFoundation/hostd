---
default: patch
---

# Fixed an inconsistency migrating the config's log directory from the v1.1.2 config file

The deprecated `Log.Path` config field was a directory, while the new `Log.File.Path` is expected to be a file path. If the log directory was set, the log file path will be correctly migrated to `hostd.log` in the directory.
