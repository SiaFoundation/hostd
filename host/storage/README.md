# Storage Manager
A Storage Manager is responsible for storing and retrieving sectors uploaded to
the host. Sectors can now come from two different sources: contracts and
temporary storage.

## Responsibilities
+ Managing stored sectors
+ Reporting storage inconsistencies
+ Deleting sectors that are no longer referenced

## Storage
The RHP packages interact directly with a storage manager to write and read
sectors from persistent storage.

### Temp Storage
Currently the storage manager also manages temporary storage, but that may not
always be the case.

# Volume Manager
In `hostd` the Volume Manager is the default implementation of a storage
manager. It is responsible for managing the storage of sectors on disk. It
stores metadata in a SQLite database and sector data in a flat file. 

## Responsibilities
+ Managing volume metadata
+ Managing sector metadata
+ Managing volume data
+ Managing temporary storage
+ Reporting disk errors
+ Migrating volume data

## Requirements
+ Stored sectors must stay consistent between storage manager, contract manager,
  and temp storage
+ Metadata must be ACID between contract revisions in RHP2 and program
finalization in RHP3.
+ Support for setting volumes to read-only
+ Support for checking on-disk consistency and reporting errors
+ Support for resizing and removing volumes
+ During resize, sectors must be able to be moved within the same volume
+ Avoid locking a folder during resize or remove operations; sectors not being
  actively moved should be readable.
+ Support for multiple volumes with dynamic sizes
+ Provide alerts and status information on long-running operations
