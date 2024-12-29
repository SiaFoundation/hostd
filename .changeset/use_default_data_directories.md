---
default: patch
---

# Use standard locations for application data

Uses standard locations for application data instead of the current directory. This brings `hostd` in line with other system services and makes it easier to manage application data.

#### Linux, FreeBSD, OpenBSD
- Configuration: `/etc/hostd`
- Data directory: `/var/lib/hostd`

#### macOS
- Configuration: `~/Library/Application Support/hostd`
- Data directory: `~/Library/Application Support/hostd`

#### Windows
- Configuration: `%APPDATA%\SiaFoundation\hostd`
- Data directory: `%APPDATA%\SiaFoundation\hostd`
