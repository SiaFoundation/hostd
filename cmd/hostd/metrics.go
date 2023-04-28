package main

type discardMetricReporter struct{}

func (mr discardMetricReporter) Report(metric any) error {
	return nil
}
