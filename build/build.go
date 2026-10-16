// Package build contains build-time information.
package build

//go:generate go run gen.go

import "time"

// Commit returns the commit hash of hostd
func Commit() string {
	return commit
}

// Version returns the version of hostd
func Version() string {
	return version
}

// Time returns the build timestamp recorded in the build metadata. For tagged
// versions, this is the commit time; development builds use the generation time.
func Time() time.Time {
	return time.Unix(buildTime, 0)
}
