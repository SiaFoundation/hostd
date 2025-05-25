package version

import (
	"context"
	"fmt"
	"time"

	"github.com/google/go-github/github"
	"go.sia.tech/hostd/v2/alerts"
	"go.sia.tech/hostd/v2/build"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	org  = "SiaFoundation"
	repo = "hostd"
)

// An Alerter is an interface for registering alerts.
type Alerter interface {
	Register(a alerts.Alert)
}

var alertID = frand.Entropy256()

// latestRelease fetches the latest release from a GitHub repository.
func latestRelease(ctx context.Context, org, repo string) (string, error) {
	client := github.NewClient(nil)

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	release, _, err := client.Repositories.GetLatestRelease(ctx, org, repo)
	if err != nil {
		return "", err
	} else if release.Name == nil {
		return "", fmt.Errorf("no release found for %s/%s", org, repo)
	}
	return *release.Name, nil
}

func alertVersion(ctx context.Context, a Alerter, log *zap.Logger) error {
	log = log.With(zap.String("current", build.Version()))

	var version semVer
	if err := version.UnmarshalText([]byte(build.Version())); err != nil {
		return fmt.Errorf("failed to parse current version %q: %w", build.Version(), err)
	}

	latestStr, err := latestRelease(ctx, org, repo)
	if err != nil {
		return fmt.Errorf("failed to get latest release: %w", err)
	}
	log.Debug("latest release", zap.String("version", latestStr))
	var latest semVer
	if err := latest.UnmarshalText([]byte(latestStr)); err != nil {
		return fmt.Errorf("failed to parse latest version %q: %w", latestStr, err)
	} else if latest.Cmp(version) > 0 {
		return nil
	}

	a.Register(alerts.Alert{
		ID:       alertID,
		Severity: alerts.SeverityInfo,
		Category: "update",
		Message:  "Update available",
		Data: map[string]any{
			"current": version.String(),
			"latest":  latest.String(),
		},
		Timestamp: time.Now(),
	})
	return nil
}

// RunVersionCheck periodically checks for updates and alerts the user
// when a new version is available.
func RunVersionCheck(ctx context.Context, a Alerter, log *zap.Logger) {
	if build.Version() == "" {
		log.Debug("no version available, version checks disabled")
		return
	}
	t := time.NewTicker(6 * time.Hour)
	defer t.Stop()
	ch := make(chan struct{}, 1)
	ch <- struct{}{} // check for updates on startup
	for {
		select {
		case <-ctx.Done():
			return
		case <-ch:
		case <-t.C:
		}
		if err := alertVersion(ctx, a, log); err != nil {
			log.Warn("failed to check version", zap.Error(err))
		}
	}
}
