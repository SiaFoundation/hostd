package settings

import (
	"strings"
	"testing"
)

func TestValidateHostname(t *testing.T) {
	tests := []struct {
		addr   string
		valid  bool
		errStr string
	}{
		{"", false, "empty hostname"},
		{"localhost", false, "hostname cannot be localhost"},
		{"foo.bar:9982", false, "hostname should not contain a port"},
		{"192.168.1.1", false, "only public IP addresses allowed"},
		{"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:9982", false, "hostname should not contain a port"},
		{"[2001:0db8:85a3:0000:0000:8a2e:0370:7334]", false, `hostname must not start with "[" or end with "]"`},
		{"2001:0db8:85a3:0000:0000:8a2e:0370:7334", true, ""},
		{"55.123.123.123", true, ""},
		{"foo.bar", true, ""},
	}

	for _, test := range tests {
		t.Run(test.addr, func(t *testing.T) {
			err := validateHostname(test.addr)
			if err != nil {
				if test.valid {
					t.Fatalf("expected valid host, got %v", err)
				} else if !strings.Contains(err.Error(), test.errStr) {
					t.Fatalf("expected %v, got %v", test.errStr, err)
				}
			} else if !test.valid {
				t.Fatalf("expected %v, got %v", test.errStr, err)
			}
		})
	}
}
