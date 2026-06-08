package version

import "testing"

func TestBanner(t *testing.T) {
	if got := Banner(); got != "PrismGo Framework 0.1.0" {
		t.Fatalf("Banner() = %q", got)
	}
}
