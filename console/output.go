package console

import (
	"io"
	"os"
	"regexp"
	"strings"

	"golang.org/x/term"
)

var ansiTermPattern = regexp.MustCompile(`^((screen|xterm|vt100|vt220|putty|rxvt|ansi|cygwin|linux).*)|(.*-256(color)?(-bce)?)$`)

// ResolveOutputOptions applies Laravel/Symfony-style console decoration rules.
// Explicit --ansi and --no-ansi flags win; otherwise ANSI is auto-detected from
// the output stream and environment, matching Symfony StreamOutput semantics.
func ResolveOutputOptions(out io.Writer, ansiSet, noANSISet, quiet, silent bool) OutputOptions {
	return OutputOptions{
		ANSI:   ResolveANSI(out, ansiSet, noANSISet),
		Quiet:  quiet,
		Silent: silent,
	}
}

// ResolveANSI returns whether ANSI decoration should be enabled.
func ResolveANSI(out io.Writer, ansiSet, noANSISet bool) bool {
	if noANSISet {
		return false
	}
	if ansiSet {
		return true
	}
	return SupportsANSI(out)
}

// SupportsANSI mirrors the practical parts of Symfony Console's color support
// detection for non-forced output.
func SupportsANSI(out io.Writer) bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if forceColorEnabled(os.Getenv("FORCE_COLOR")) {
		return true
	}
	if !isTerminalWriter(out) && !isMingwTerminal() {
		return false
	}
	if os.Getenv("COLORTERM") != "" || os.Getenv("ANSICON") != "" || os.Getenv("ConEmuANSI") == "ON" || os.Getenv("TERM_PROGRAM") == "Hyper" {
		return true
	}
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	return ansiTermPattern.MatchString(os.Getenv("TERM"))
}

// forceColorEnabled 解析 FORCE_COLOR 的显式开启语义。
// 逻辑说明：NO_COLOR 仍在外层保持最高优先级；这里只负责识别哪些值表示“强制开启颜色”，
// 并把 0/false/no/off 这类显式关闭值排除掉，以对齐常见 CLI 生态约定。
func forceColorEnabled(value string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(value))
	if trimmed == "" || trimmed == "0" || trimmed == "false" || trimmed == "no" || trimmed == "off" {
		return false
	}
	return true
}

func isTerminalWriter(out io.Writer) bool {
	file, ok := out.(*os.File)
	return ok && term.IsTerminal(int(file.Fd()))
}

func isMingwTerminal() bool {
	msystem := strings.ToUpper(os.Getenv("MSYSTEM"))
	return msystem == "MINGW32" || msystem == "MINGW64"
}
