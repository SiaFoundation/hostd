package financials

import (
	"time"

	"go.sia.tech/siad/types"
)

type (
	// A SourceID is the ID of the funding source - either a contract or
	// account ID
	SourceID [32]byte

	Record struct {
		FundSource       SourceID       `json:"fundSource"`
		Fees             types.Currency `json:"fees"`
		Storage          types.Currency `json:"storage"`
		Ingress          types.Currency `json:"ingress"`
		Egress           types.Currency `json:"egress"`
		LockedCollateral types.Currency `json:"lockedCollateral"`
		RiskedCollateral types.Currency `json:"riskedCollateral"`
		BurntCollateral  types.Currency `json:"burntCollateral"`
		Timestamp        time.Time      `json:"timestamp"`
	}
)
