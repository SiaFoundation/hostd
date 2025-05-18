---
default: minor
---

# Added periodic connection tests

Periodically tests the hosts connection using the SiaScan troubleshooting API. This provides alerts to hosts if for some reason their node is not connectable. This system relies on a central server for the test and may return occasional false positives. The server can be overridden by setting `Config.Explorer.URL`