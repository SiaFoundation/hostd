package settings

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/rhp/v4/siamux"
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
func (cm *ConfigManager) TestConnection(ctx context.Context) (explorer.TestResult, bool, error) {
	ctx, cancel, err := cm.tg.AddContext(ctx)
	if err != nil {
		return explorer.TestResult{}, false, err
	}
	defer cancel()

	if cm.explorer == nil {
		return explorer.TestResult{}, false, errors.New("connection test requires explorer")
	} else if cm.a == nil {
		return explorer.TestResult{}, false, errors.New("connection test requires alert manager")
	}

	rhp2NetAddress := cm.rhp2NetAddress()
	if rhp2NetAddress == "" {
		return explorer.TestResult{}, false, errors.New("rhp2 net address is empty")
	}
	rhp4NetAddress := cm.rhp4NetAddress()
	if rhp4NetAddress == "" {
		return explorer.TestResult{}, false, errors.New("rhp4 net address is empty")
	}

	result, err := cm.explorer.TestConnection(ctx, explorer.Host{
		PublicKey:      cm.hostKey.PublicKey(),
		RHP2NetAddress: rhp2NetAddress,
		RHP4NetAddresses: []chain.NetAddress{
			{
				Protocol: siamux.Protocol,
				Address:  rhp4NetAddress,
			},
		},
	})
	if err != nil {
		return explorer.TestResult{}, false, fmt.Errorf("failed to test connection: %w", err)
	}

	// helper to register an alert
	successful := true
	registerAlert := func(severity alerts.Severity, msg string) {
		successful = false
		cm.a.Register(alerts.Alert{
			ID:        frand.Entropy256(),
			Category:  troubleshootAlertCategory,
			Severity:  severity,
			Message:   msg,
			Timestamp: time.Now(),
		})
	}

	cm.a.DismissCategory(troubleshootAlertCategory)
	cs := cm.chain.TipState()
	if cs.Index.Height < cs.Network.HardforkV2.RequireHeight {
		if result.RHP2 != nil {
			for _, err := range result.RHP2.Errors {
				registerAlert(alerts.SeverityWarning, fmt.Sprintf("Issue testing RHP2 connection: %s", err))
			}
		}

		if result.RHP3 != nil {
			for _, err := range result.RHP3.Errors {
				registerAlert(alerts.SeverityWarning, fmt.Sprintf("Issue testing RHP3 connection: %s", err))
			}
		}
	}
	for _, protoTest := range result.RHP4 {
		for _, err := range protoTest.Errors {
			registerAlert(alerts.SeverityWarning, fmt.Sprintf("Issue testing RHP4 %q connection: %s", protoTest.NetAddress.Protocol, err))
		}
	}
	return result, successful, nil
}
