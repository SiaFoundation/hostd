package main

type discordMetricReporter struct{}

func (mr discordMetricReporter) Report(metric any) error {
	return nil
}
