//go:build testing

package contracts

const (
	// RebroadcastBuffer is the number of blocks after the negotiation height to
	// attempt to rebroadcast the contract.
	RebroadcastBuffer = 12
	// RevisionSubmissionBuffer number of blocks before the proof window to
	// submit a revision and prevent modification of the contract.
	RevisionSubmissionBuffer = 24
)
