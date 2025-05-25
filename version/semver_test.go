package version

import "testing"

func TestSemver(t *testing.T) {
	tests := []struct {
		a        string
		b        string
		expected int
	}{
		{"v1.2.3", "v1.2.3", 0},
		{"v1.2.3", "v1.2.4", -1},
		{"v1.2.4", "v1.2.3", 1},
		{"v1.2.3-beta.1", "v1.2.3", -1}, // pre-release < release
		{"v1.2.3", "v1.2.3-beta.1", 1},  // release > pre-release
		{"v1.2.3", "v1.2.3-beta.n", 1},  // release > malformed pre-release
		{"v1.2.3-beta.1", "v1.2.3-beta.2", -1},
		{"v1.2.3-alpha.1", "v1.2.3-beta.1", -1},
		{"v1.2.3-beta.1", "v1.2.3-alpha.1", 1},
		{"v1.2.3-alpha.1", "v1.2.3-alpha.2", -1},
		{"v1.2.3-alpha.1", "v1.2.3-rc1", 1},
		{"v1.2.3-alpha.a", "v1.2.3-alpha.1", -1},
		{"v1.2.3-beta.a", "v1.2.3-alpha.1", 1},
	}

	for _, test := range tests {
		var a, b semVer
		if err := a.UnmarshalText([]byte(test.a)); err != nil {
			t.Fatalf("failed to parse version %q: %v", test.a, err)
		}
		if err := b.UnmarshalText([]byte(test.b)); err != nil {
			t.Fatalf("failed to parse version %q: %v", test.b, err)
		}

		result := a.Cmp(b)
		if result != test.expected {
			t.Errorf("expected %d for comparison of %q and %q, got %d", test.expected, test.a, test.b, result)
		}
	}
}
