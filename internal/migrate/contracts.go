package migrate

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"time"

	"gitlab.com/NebulousLabs/bolt"
	rhp2 "go.sia.tech/core/rhp/v2"
	"go.sia.tech/core/types"
	"go.sia.tech/hostd/host/contracts"
	stypes "go.sia.tech/siad/types"
	"go.uber.org/zap"
)

type (
	storageObligation struct {
		// Storage obligations are broken up into ordered atomic sectors that are
		// exactly 4MiB each. By saving the roots of each sector, storage proofs
		// and modifications to the data can be made inexpensively by making use of
		// the merkletree.CachedTree. Sectors can be appended, modified, or deleted
		// and the host can recompute the Merkle root of the whole file without
		// much computational or I/O expense.
		SectorRoots []types.Hash256

		// The negotiation height specifies the block height at which the file
		// contract was negotiated. If the origin transaction set is not accepted
		// onto the blockchain quickly enough, the contract is pruned from the
		// host. The origin and revision transaction set contain the contracts +
		// revisions as well as all parent transactions. The parents are necessary
		// because after a restart the transaction pool may be emptied out.
		NegotiationHeight      uint64
		OriginTransactionSet   []stypes.Transaction
		RevisionTransactionSet []stypes.Transaction
	}
)

func migrateContracts(ctx context.Context, db Store, dir string, maxHeight uint64, log *zap.Logger) (success, skipped, failed int, err error) {
	crapDB, err := bolt.Open(filepath.Join(dir, "host", "host.db"), 0600, &bolt.Options{Timeout: 3 * time.Second})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("failed to open host database: %w", err)
	}
	defer crapDB.Close()

	contractBucket := []byte("BucketStorageObligations")

	// iterate over all contracts
	err = crapDB.View(func(tx *bolt.Tx) error {
		select {
		case <-ctx.Done():
			return ctx.Err() // abort
		default:
		}

		bucket := tx.Bucket(contractBucket)
		if bucket == nil {
			return fmt.Errorf("bucket %s not found", contractBucket)
		}

		return bucket.ForEach(func(k, v []byte) error {
			// contract parsing/corruption errors are logged and ignored
			fcID := (types.FileContractID)(k)

			log := log.With(zap.Stringer("contractID", fcID))

			var so storageObligation
			if err := json.Unmarshal(v, &so); err != nil {
				log.Warn("failed to decode storage obligation", zap.Error(err))
				failed++
				return nil
			} else if len(so.RevisionTransactionSet) == 0 {
				// all contracts should have at least one revision
				log.Warn("contract has no revisions")
				failed++
				return nil
			}

			var revisionTxn types.Transaction
			convertToCore(so.RevisionTransactionSet[len(so.RevisionTransactionSet)-1], &revisionTxn)
			if len(revisionTxn.FileContractRevisions) != 1 {
				log.Warn("revision transaction has no revisions")
				failed++
				return nil
			} else if len(revisionTxn.Signatures) != 2 {
				log.Warn("revision transaction has no signatures")
				failed++
				return nil
			}

			revision := revisionTxn.FileContractRevisions[0]
			// validate that the revision makes sense
			if revision.ParentID != fcID {
				log.Warn("revision parent id does not match contract id", zap.Stringer("parentID", revision.ParentID))
				failed++
				return nil
			} else if revision.WindowEnd < maxHeight {
				log.Debug("skipping expired contract", zap.Uint64("proofWindowEnd", revision.WindowEnd))
				skipped++
				return nil
			}

			expectedRoot := rhp2.MetaRoot(so.SectorRoots)
			if revision.FileMerkleRoot != expectedRoot {
				log.Warn("revision merkle root does not match expected root", zap.Stringer("root", revision.FileMerkleRoot), zap.Stringer("expected", expectedRoot))
				failed++
				return nil
			}

			expectedFileSize := uint64(len(so.SectorRoots)) * rhp2.SectorSize
			if revision.Filesize != expectedFileSize {
				log.Warn("revision filesize does not match expected filesize", zap.Uint64("size", revision.Filesize), zap.Uint64("expected", expectedFileSize))
				failed++
				return nil
			}

			signedRevision := contracts.SignedRevision{
				Revision:        revision,
				RenterSignature: types.Signature(revisionTxn.Signatures[0].Signature),
				HostSignature:   types.Signature(revisionTxn.Signatures[1].Signature),
			}

			var formationTxnSet []types.Transaction
			for _, stxn := range so.OriginTransactionSet {
				var txn types.Transaction
				convertToCore(stxn, &txn)
				formationTxnSet = append(formationTxnSet, txn)
			}

			// contract is added with no financials or collateral because we
			// can't trust siad to have done the math correctly
			if err := db.SiadMigrateContract(signedRevision, so.NegotiationHeight, formationTxnSet, so.SectorRoots); err != nil {
				return fmt.Errorf("failed to revise contract: %w", err)
			}
			log.Info("migrated contract", zap.Int("sectors", len(so.SectorRoots)))
			success++
			return nil
		})
	})

	return
}
