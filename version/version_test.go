package version

import "testing"

func TestBanner(t *testing.T) {
	// Build the expected banner from exported version fields so release bumps update one source of truth.
	want := Name + " Framework " + Framework
	if got := Banner(); got != want {
		t.Fatalf("Banner() = %q", got)
	}
}
