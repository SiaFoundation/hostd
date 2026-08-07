---
default: patch
---

# Fix volume usage accounting going negative

Fixed two paths that could leave a volume's `used_sectors` counter below the
number of sectors actually stored in it. Once the counter went negative the
background sector pruner panicked, which exited the daemon without writing
anything to the log and recurred on every restart.

Hosts already affected can repair the counters by running `hostd recalculate`
against their database while the host is stopped.
