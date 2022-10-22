package main

import (
	"fmt"

	"go.sia.tech/hostd/rhp/v2"
)

type stdoutmetricReporter struct{}

func (mr stdoutmetricReporter) Report(metric any) error {
	switch v := metric.(type) {
	case rhp.EventSessionStart:
		fmt.Printf("renter session started")
	case rhp.EventSessionEnd:
		fmt.Printf("renter session ended")
	case rhp.EventRPCStart:
		fmt.Printf("rpc started", v.RPC)
	case rhp.EventRPCEnd:
		fmt.Printf("rpc ended", v.RPC)
	}
	return nil
}
