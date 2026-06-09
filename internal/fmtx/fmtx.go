package fmtx

import (
	"fmt"
	"io"
)

func Fprint(w io.Writer, a ...any) {
	_, _ = fmt.Fprint(w, a...) //nolint:errcheck
}

func Fprintf(w io.Writer, format string, a ...any) {
	_, _ = fmt.Fprintf(w, format, a...) //nolint:errcheck
}

func Fprintln(w io.Writer, a ...any) {
	_, _ = fmt.Fprintln(w, a...) //nolint:errcheck
}
