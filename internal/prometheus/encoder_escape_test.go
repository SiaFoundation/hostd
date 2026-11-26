package prometheus

import (
	"strings"
	"testing"
)

type labelStringer string

func (s labelStringer) String() string { return string(s) }

func TestMetricEscapesLabels(t *testing.T) {
	value := "disk\\path\n\"quoted\""
	for _, v := range []any{value, []byte(value), labelStringer(value)} {
		m := Metric{Name: "test_metric", Labels: map[string]any{"label": v}, Value: 1}
		var sb strings.Builder
		if err := m.encode(&sb); err != nil {
			t.Fatal(err)
		}
		want := `test_metric{label="disk\\path\n\"quoted\""} 1`
		if sb.String() != want {
			t.Errorf("label %T: got %q, want %q", v, sb.String(), want)
		}
	}
}
