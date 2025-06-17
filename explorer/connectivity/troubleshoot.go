package connectivity

import (
	"context"
	"fmt"
	"time"

	"go.sia.tech/hostd/v2/alerts"
	"go.sia.tech/hostd/v2/explorer"
	"lukechampine.com/frand"
)

const (
	troubleshootAlertCategory = "connectionTest"
)

// TestConnection checks if the connection to the host is valid.
// If any errors are returned, it will register alerts with the
// alert manager.
// It returns a TestResult containing the results of the connection test and
// a boolean indicating whether there were any errors with any of the results.
func (m *Manager) TestConnection(ctx context.Context) (explorer.TestResult, bool, error) {
	ctx, cancel, err := m.tg.AddContext(ctx)
	if err != nil {
		return explorer.TestResult{}, false, err
	}
	defer cancel()

	result, err := m.explorer.TestConnection(ctx, explorer.Host{
		PublicKey:        m.hostKey,
		RHP4NetAddresses: m.settings.RHP4NetAddresses(),
	})
	if err != nil {
		return explorer.TestResult{}, false, fmt.Errorf("failed to test connection: %w", err)
	}

	// alert helpers
	dismissAlerts := func() {
		if m.alerts == nil {
			return
		}
		m.alerts.DismissCategory(troubleshootAlertCategory)
	}
	registerAlert := func(severity alerts.Severity, msg string) {
		if m.alerts == nil {
			return
		}
		m.alerts.Register(alerts.Alert{
			ID:        frand.Entropy256(),
			Category:  troubleshootAlertCategory,
			Severity:  severity,
			Message:   msg,
			Timestamp: time.Now(),
		})
	}

	dismissAlerts()
	successful := true
	for _, protoTest := range result.RHP4 {
		for _, err := range protoTest.Errors {
			successful = false
			registerAlert(alerts.SeverityWarning, fmt.Sprintf("Issue testing RHP4 %q connection: %s", protoTest.NetAddress.Protocol, err))
		}
	}
	return result, successful, nil
}
