package console

import (
	"bytes"
	"io"
	"testing"
)

func TestResolveANSIMatchesSymfonyForcedAndEnvironmentRules(t *testing.T) {
	t.Setenv("NO_COLOR", "")
	t.Setenv("FORCE_COLOR", "")
	t.Setenv("TERM", "")
	t.Setenv("COLORTERM", "")
	t.Setenv("ANSICON", "")
	t.Setenv("ConEmuANSI", "")
	t.Setenv("TERM_PROGRAM", "")
	t.Setenv("MSYSTEM", "")

	if ResolveANSI(&bytes.Buffer{}, false, false) {
		t.Fatal("expected buffered output without color environment to stay undecorated")
	}

	t.Setenv("FORCE_COLOR", "1")
	if !ResolveANSI(&bytes.Buffer{}, false, false) {
		t.Fatal("expected FORCE_COLOR to enable decoration")
	}

	t.Setenv("NO_COLOR", "1")
	if ResolveANSI(&bytes.Buffer{}, false, false) {
		t.Fatal("expected NO_COLOR to disable auto decoration")
	}
	if !ResolveANSI(&bytes.Buffer{}, true, false) {
		t.Fatal("expected explicit --ansi to override NO_COLOR")
	}
	if ResolveANSI(&bytes.Buffer{}, true, true) {
		t.Fatal("expected explicit --no-ansi to override --ansi")
	}

	t.Setenv("NO_COLOR", "")
	t.Setenv("FORCE_COLOR", "0")
	if ResolveANSI(&bytes.Buffer{}, false, false) {
		t.Fatal("expected FORCE_COLOR=0 to keep auto decoration disabled")
	}
}

func TestResolveOutputOptionsCarriesVerbosityAndDecoration(t *testing.T) {
	opts := ResolveOutputOptions(io.Discard, true, false, true, false)
	if !opts.ANSI || !opts.Quiet || opts.Silent {
		t.Fatalf("ResolveOutputOptions = %+v", opts)
	}
}
