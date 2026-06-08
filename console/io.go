package console

import (
	"bufio"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/mgutz/ansi"
	consolecontract "github.com/prismgo/framework/contracts/console"
	"golang.org/x/term"
)

// IO 提供统一的命令行交互与输出能力。
type IO = consolecontract.IO

// Progress 描述一个最小进度条接口。
type Progress = consolecontract.Progress

type terminalIO struct {
	in      io.Reader
	reader  *bufio.Reader
	out     io.Writer
	errOut  io.Writer
	output  OutputOptions
	errOpts OutputOptions
}

type outputWriterProvider interface {
	Output() io.Writer
}

type errorOutputWriterProvider interface {
	ErrorOutput() io.Writer
}

type simpleProgress struct {
	out     io.Writer
	total   int
	current int
}

const (
	consoleStyleInfo     = "info"
	consoleStyleComment  = "comment"
	consoleStyleQuestion = "question"
	consoleStyleSuccess  = "success"
	consoleStyleWarn     = "warn"
	consoleStyleWarning  = "warning"
	consoleStyleError    = "error"
)

var consoleANSIStyles = map[string]string{
	consoleStyleInfo:     "green",
	consoleStyleComment:  "yellow",
	consoleStyleQuestion: "black:cyan",
	consoleStyleSuccess:  "white:green",
	consoleStyleWarn:     "yellow",
	consoleStyleWarning:  "yellow",
	consoleStyleError:    "white:red",
}

// NewIO 创建默认终端 IO 实现。
func NewIO(in io.Reader, out io.Writer, errOut io.Writer) IO {
	return NewIOWithOutputOptions(in, out, errOut, ResolveOutputOptions(out, false, false, false, false))
}

// NewIOWithOutputOptions creates terminal IO with caller-resolved rendering
// options, used by the kernel after parsing global console flags.
func NewIOWithOutputOptions(in io.Reader, out io.Writer, errOut io.Writer, output OutputOptions) IO {
	return &terminalIO{in: in, reader: bufio.NewReader(in), out: out, errOut: errOut, output: output, errOpts: output}
}

// OutputWriter returns the stdout writer for IO implementations that expose it.
// It exists for framework formatters that need to stream structured output
// while still respecting tests and callers that inject command IO.
func OutputWriter(ioo IO) io.Writer {
	if provider, ok := ioo.(outputWriterProvider); ok && provider.Output() != nil {
		return provider.Output()
	}
	if terminal, ok := ioo.(*terminalIO); ok && terminal.out != nil {
		return terminal.out
	}
	return io.Discard
}

// ErrorOutputWriter returns the stderr writer for IO implementations that expose it.
func ErrorOutputWriter(ioo IO) io.Writer {
	if provider, ok := ioo.(errorOutputWriterProvider); ok && provider.ErrorOutput() != nil {
		return provider.ErrorOutput()
	}
	if terminal, ok := ioo.(*terminalIO); ok && terminal.errOut != nil {
		return terminal.errOut
	}
	return io.Discard
}

// OutputOptionsForIO returns the stdout rendering options attached to IO.
func OutputOptionsForIO(ioo IO) OutputOptions {
	if terminal, ok := ioo.(*terminalIO); ok {
		return terminal.output
	}
	return OutputOptions{}
}

func (ioo *terminalIO) Output() io.Writer {
	return ioo.out
}

func (ioo *terminalIO) ErrorOutput() io.Writer {
	return ioo.errOut
}

func (ioo *terminalIO) Line(message string, style ...string) {
	writeLine(ioo.out, message, firstStyle(style...), ioo.output)
}

func (ioo *terminalIO) Info(message string) {
	writeLine(ioo.out, message, consoleStyleInfo, ioo.output)
}

func (ioo *terminalIO) Comment(message string) {
	writeLine(ioo.out, message, consoleStyleComment, ioo.output)
}

func (ioo *terminalIO) Question(message string) {
	writeLine(ioo.out, message, consoleStyleQuestion, ioo.output)
}

func (ioo *terminalIO) Success(message string) {
	writeLine(ioo.out, message, consoleStyleSuccess, ioo.output)
}

func (ioo *terminalIO) Warn(message string) {
	writeLine(ioo.errOut, message, consoleStyleWarn, ioo.errOpts)
}

func (ioo *terminalIO) Error(message string) {
	writeLine(ioo.errOut, message, consoleStyleError, ioo.errOpts)
}

func (ioo *terminalIO) Alert(message string) {
	writeAlert(ioo.out, message, ioo.output)
}

func (ioo *terminalIO) Ask(question string, defaultValue ...string) (string, error) {
	prompt := strings.TrimSpace(question)
	if len(defaultValue) > 0 && strings.TrimSpace(defaultValue[0]) != "" {
		prompt += " [default: " + strings.TrimSpace(defaultValue[0]) + "]"
	}
	fmt.Fprint(ioo.out, prompt+": ")
	answer, err := ioo.reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return "", err
	}
	answer = strings.TrimSpace(answer)
	if answer == "" && len(defaultValue) > 0 {
		return strings.TrimSpace(defaultValue[0]), nil
	}
	return answer, nil
}

func (ioo *terminalIO) Confirm(question string, defaultYes bool) (bool, error) {
	suffix := "[y/N]"
	defaultValue := "n"
	if defaultYes {
		suffix = "[Y/n]"
		defaultValue = "y"
	}
	answer, err := ioo.Ask(strings.TrimSpace(question)+" "+suffix, defaultValue)
	if err != nil {
		return false, err
	}
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return true, nil
	case "n", "no":
		return false, nil
	default:
		return false, nil
	}
}

func (ioo *terminalIO) Choice(question string, options []string, defaultValue ...string) (string, error) {
	config := ChoiceOptions{}
	if len(defaultValue) > 0 {
		config.Defaults = []string{defaultValue[0]}
	}
	choices, err := ioo.ChoiceWithOptions(question, options, config)
	if err != nil {
		return "", err
	}
	if len(choices) == 0 {
		return "", nil
	}
	return choices[0], nil
}

func (ioo *terminalIO) ChoiceWithOptions(question string, options []string, config ChoiceOptions) ([]string, error) {
	if len(options) == 0 {
		return nil, fmt.Errorf("choice options cannot be empty")
	}
	attempts := config.Attempts
	if attempts <= 0 {
		attempts = 1
	}
	for index, option := range options {
		fmt.Fprintf(ioo.out, "%d) %s\n", index+1, option)
	}
	defaultValue := ""
	if len(config.Defaults) > 0 {
		defaultValue = strings.Join(config.Defaults, ",")
	}
	var last string
	for attempt := 0; attempt < attempts; attempt++ {
		var (
			answer string
			err    error
		)
		if defaultValue != "" {
			answer, err = ioo.Ask(question, defaultValue)
		} else {
			answer, err = ioo.Ask(question)
		}
		if err != nil {
			return nil, err
		}
		last = answer
		choices, ok := resolveChoices(answer, options, config.Multiple)
		if ok {
			return choices, nil
		}
	}
	return nil, fmt.Errorf("invalid choice %q", last)
}

func (ioo *terminalIO) Anticipate(question string, choices []string, defaultValue ...string) (string, error) {
	// 非 TTY 或 raw mode 不可用时保持 Ask 行为；当前实现不引入额外 prompt 依赖。
	file, ok := ioo.in.(interface{ Fd() uintptr })
	if !ok || !term.IsTerminal(int(file.Fd())) {
		return ioo.Ask(question, defaultValue...)
	}
	return ioo.Ask(question, defaultValue...)
}

func (ioo *terminalIO) NewLine(count ...int) {
	lines := 1
	if len(count) > 0 {
		lines = count[0]
	}
	for i := 0; i < lines; i++ {
		fmt.Fprintln(ioo.out)
	}
}

func resolveChoices(answer string, options []string, multiple bool) ([]string, bool) {
	parts := []string{answer}
	if multiple {
		parts = strings.Split(answer, ",")
	}
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		matched := ""
		for _, option := range options {
			if part == option {
				matched = option
				break
			}
		}
		if matched == "" {
			for index, option := range options {
				if part == fmt.Sprintf("%d", index+1) {
					matched = option
					break
				}
			}
		}
		if matched == "" {
			return nil, false
		}
		result = append(result, matched)
	}
	if len(result) == 0 {
		return nil, false
	}
	return result, true
}

func (ioo *terminalIO) Secret(question string) (string, error) {
	file, ok := ioo.in.(interface{ Fd() uintptr })
	if !ok || !term.IsTerminal(int(file.Fd())) {
		return ioo.Ask(question)
	}
	fmt.Fprint(ioo.out, strings.TrimSpace(question)+": ")
	bytes, err := term.ReadPassword(int(file.Fd()))
	fmt.Fprintln(ioo.out)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(bytes)), nil
}

func (ioo *terminalIO) Table(headers []string, rows [][]string) error {
	writer := tabwriter.NewWriter(ioo.out, 0, 0, 2, ' ', 0)
	if len(headers) > 0 {
		fmt.Fprintln(writer, strings.Join(headers, "\t"))
	}
	for _, row := range rows {
		fmt.Fprintln(writer, strings.Join(row, "\t"))
	}
	return writer.Flush()
}

func (ioo *terminalIO) Progress(total int) Progress {
	return &simpleProgress{out: ioo.out, total: total}
}

func (p *simpleProgress) Advance(step int) {
	if step <= 0 {
		step = 1
	}
	p.current += step
	if p.total > 0 {
		fmt.Fprintf(p.out, "\r[%d/%d]", p.current, p.total)
		return
	}
	fmt.Fprintf(p.out, "\r[%d]", p.current)
}

func (p *simpleProgress) Finish() {
	fmt.Fprintln(p.out)
}

func firstStyle(style ...string) string {
	if len(style) == 0 {
		return ""
	}
	return style[0]
}

func writeLine(writer io.Writer, message string, style string, opts OutputOptions) {
	if opts.ANSI {
		if ansiStyle, ok := consoleANSIStyles[style]; ok {
			fmt.Fprintln(writer, ansi.Color(message, ansiStyle))
			return
		}
	}
	fmt.Fprintln(writer, message)
}

func writeAlert(writer io.Writer, message string, opts OutputOptions) {
	border := strings.Repeat("*", len(message)+12)
	writeLine(writer, border, consoleStyleComment, opts)
	writeLine(writer, "*     "+message+"     *", consoleStyleComment, opts)
	writeLine(writer, border, consoleStyleComment, opts)
}
