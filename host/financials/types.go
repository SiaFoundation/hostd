package financials

import (
	"fmt"
	"time"

	"go.sia.tech/core/types"
)

const (
	// FundSourceContract identifies a contract as the source of funds
	FundSourceContract = "contract"
	// FundSourceAccount identifies an account as the source of funds
	FundSourceAccount = "account"
)

type (
	// A FundSource identifies the source of a financial record
	FundSource struct {
		ID   [32]byte
		Type string
	}

	// A FundingRecord records a transfer of funds between a source contract and
	// a destination account.
	FundingRecord struct {
		// Source is the source of the funds. It must be a contract.
		Source FundSource `json:"source"`
		// Destination is the destination of the funds. It must be an account.
		Destination FundSource     `json:"destination"`
		Amount      types.Currency `json:"amount"`
		// Reverted indicates whether the funding source was reverted due to a
		// block reorg or other consensus issue.
		Reverted  bool      `json:"reverted"`
		Timestamp time.Time `json:"timestamp"`
	}

	// A Record records spending from a funding source
	Record struct {
		Source  FundSource     `json:"source"`
		Egress  types.Currency `json:"egress"`
		Ingress types.Currency `json:"ingress"`
		Fees    types.Currency `json:"fees"`
		Storage types.Currency `json:"storage"`

		Timestamp time.Time `json:"timestamp"`
	}

	// Revenue tracks the host's earnings from all possible sources for a given
	// period
	Revenue struct {
		Storage types.Currency `json:"storage"`
		Ingress types.Currency `json:"ingress"`
		Egress  types.Currency `json:"egress"`
		Fees    types.Currency `json:"fees"`

		// AccountDrift tracks funds that were transferred to an account but are
		// no longer backed by a contract due to a reorg or other consensus
		// event. Once funds are transferred to an ephemeral account, the
		// financial records cannot be cleanly reverted. The revenue is,
		// basically, lost.
		AccountDrift types.Currency `json:"accountDrift"`

		Timestamp time.Time `json:"timestamp"`
	}
)

// String returns a string representation of the fund source
func (fs FundSource) String() string {
	return fmt.Sprintf("%s:%x", fs.Type, fs.ID)
}

// UnmarshalText unmarshals a fund source from a string
func (fs FundSource) UnmarshalText(text []byte) error {
	_, err := fmt.Sscanf(string(text), "%s:%x", &fs.Type, &fs.ID)
	return err
}

// MarshalText marshals a fund source to a string
func (fs FundSource) MarshalText() ([]byte, error) {
	return []byte(fs.String()), nil
}
