---
default: patch
---

# Fix volume usage accounting going negative

Fixed two paths that could leave a volume's `used_sectors` counter below the
number of sectors actually stored in it. Once the counter went negative the
background sector pruner panicked, which exited the daemon without writing
anything to the log and recurred on every restart.

Also fixed `hostd recalculate` skipping volumes that have no sectors left,
which meant it could report success without repairing a volume whose rows had
all been deleted by an interrupted removal.

Hosts already affected can repair the counters by running `hostd recalculate`
against their database while the host is stopped.
