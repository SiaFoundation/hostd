package alerts

import "go.sia.tech/core/types"

// A NoOpAlerter is an Alerter that does nothing.
type NoOpAlerter struct{}

// Register implements the Alerter interface.
func (NoOpAlerter) Register(Alert) {}

// Dismiss implements the Alerter interface.
func (NoOpAlerter) Dismiss(...types.Hash256) {}

var _ Alerter = NoOpAlerter{}

// NewNop returns a new NoOpAlerter.
func NewNop() NoOpAlerter {
	return NoOpAlerter{}
}
