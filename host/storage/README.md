# Storage Manager
The Storage Manager is responsible for storing and retrieving sectors uploaded
to the host and managing the volumes where sectors are stored. Sectors can now
come from two different sources: contracts and temporary storage. It also needs
to handle duplicate sectors. In `siad` the manager stores a uint32 reference
counter, but it would be better for consistency to query the number of
references remaining directly from the database before marking a location as
free.


## Responsibilities
+ Managing volume metadata
+ Managing sector metadata
+ Managing volume data
+ Managing temporary storage
+ Reporting disk errors
+ Migrating volume data

### Contract Storage
The RHP packages currently interact with the storage manager only through the 
contract manager. The contract manager is also responsible for deleting sectors
when a contract expires.

### Temp Storage
My initial designs have had the storage manager also be in charge of the new
temporary storage. That means the storage manager needs to keep track of the
expiration height and remove expired data. However, this may be better as a
separate component. Open to ideas.

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

## Persistence
I've been working on refactoring and migrating hostd's metadata into a
single SQLite database. Not strictly necessary for an MVP, but something to keep
in mind. The storage manager should also store volume metadata in the same
database to simplify transactional guarantees and metadata consistency between
contract roots and volume metadata. Deletion becomes easier as well, since we
can query references instead of relying on a counter.

## Compatibility
We should be able to keep the same sparse file format for the data, but the
metadata storage needs to change and the sector overflow file needs to be
removed.

During migration, `hostd` should recalculate the virtual sector count from the
contract sector roots rather than the folder metadata. Hosts may lose some data,
but it shouldn't cause them to fail additional contracts.
