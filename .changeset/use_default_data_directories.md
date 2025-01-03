---
default: major
---

# Use standard locations for application data

Uses standard locations for application data instead of the current directory. This brings `hostd` in line with other system services and makes it easier to manage application data.

#### Linux, FreeBSD, OpenBSD
- Configuration: `/etc/hostd/hostd.yml`
- Data directory: `/var/lib/hostd`

#### macOS
- Configuration: `~/Library/Application Support/hostd.yml`
- Data directory: `~/Library/Application Support/hostd`

#### Windows
- Configuration: `%APPDATA%\SiaFoundation\hostd.yml`
- Data directory: `%APPDATA%\SiaFoundation\hostd`

#### Docker
- Configuration: `/data/hostd.yml`
- Data directory: `/data`