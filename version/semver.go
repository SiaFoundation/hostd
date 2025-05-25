package version

import (
	"fmt"
	"strconv"
	"strings"
)

// A semVer is a semantic version string.
type semVer struct {
	version [3]byte
	suffix  string
}

// String returns the string representation of the semantic version.
func (v semVer) String() string {
	if v.suffix != "" {
		return fmt.Sprintf("v%d.%d.%d-%s", v.version[0], v.version[1], v.version[2], v.suffix)
	}
	return fmt.Sprintf("v%d.%d.%d", v.version[0], v.version[1], v.version[2])
}

// Suffix returns the suffix of the semantic version.
func (v semVer) Suffix() string {
	return v.suffix
}

// Cmp compares two semantic versions.
// Returns -1 if a < b, 0 if a == b, 1 if a > b
func (v semVer) Cmp(b semVer) int {
	// Compare two semantic versions
	switch {
	case v.version[0] != b.version[0]:
		return int(v.version[0]) - int(b.version[0])
	case v.version[1] != b.version[1]:
		return int(v.version[1]) - int(b.version[1])
	case v.version[2] != b.version[2]:
		return int(v.version[2]) - int(b.version[2])
	case v.suffix == "" && b.suffix != "":
		return 1 // v is a release version, b is a pre-release version
	case v.suffix != "" && b.suffix == "":
		return -1 // v is a pre-release version, b is a release version
	case v.suffix != "" && b.suffix != "":
		return cmpSuffix(v.suffix, b.suffix)
	default:
		return 0
	}
}

// UnmarshalText implements encoding.TextUnmarshaler
func (v *semVer) UnmarshalText(buf []byte) error {
	if len(buf) == 0 {
		return fmt.Errorf("empty version string")
	}
	version := string(buf)
	if version[0] != 'v' {
		return fmt.Errorf("invalid version format: %s", version)
	}

	var suffix string
	version = version[1:] // Remove the leading 'v'
	if suffixPos := strings.Index(version, "-"); suffixPos >= 0 {
		// remove optional suffix
		suffix = strings.ToLower(version[suffixPos+1:])
		version = version[:suffixPos]
	}

	parts := strings.Split(version, ".")
	if len(parts) != 3 {
		return fmt.Errorf("invalid version format: %s", version)
	}
	major, err := strconv.ParseUint(parts[0], 10, 8)
	if err != nil {
		return fmt.Errorf("invalid major version: %s", parts[0])
	}

	minor, err := strconv.ParseUint(parts[1], 10, 8)
	if err != nil {
		return fmt.Errorf("invalid minor version: %s", parts[1])
	}

	patch, err := strconv.ParseUint(parts[2], 10, 8)
	if err != nil {
		return fmt.Errorf("invalid patch version: %s", parts[2])
	}
	v.version = [3]byte{byte(major), byte(minor), byte(patch)}
	v.suffix = suffix
	return nil
}

func cmpSuffix(a, b string) int {
	if a == b {
		return 0
	}

	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")

	switch {
	case len(aParts) != 2 && len(bParts) != 2:
		// neither suffix is in the expected format, treat them as equal
		return 0
	case len(aParts) != 2:
		// a suffix is not in the expected format, treat it as less than b
		return -1
	case len(bParts) != 2:
		// b suffix is not in the expected format, treat it as greater than a
		return 1
	}

	suffixWeights := map[string]int{
		"alpha": 1,
		"beta":  2,
	}

	splitSuffix := func(s string) (w, n int) {
		parts := strings.Split(s, ".")
		if len(parts) != 2 {
			return 0, 0 // not a valid suffix
		}
		w, ok := suffixWeights[parts[0]]
		if !ok {
			return 0, 0 // unknown suffix, treat as less than known ones
		}
		n, err := strconv.Atoi(parts[1])
		if err != nil {
			return w, 0 // if the number part is invalid, treat it as zero
		}
		return w, n
	}

	aw, an := splitSuffix(a)
	bw, bn := splitSuffix(b)

	switch {
	case aw > bw:
		return 1
	case aw < bw:
		return -1
	case an < bn:
		return -1
	case an > bn:
		return 1
	default:
		return 0
	}
}
