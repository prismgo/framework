// Package console 提供命令行辅助方法与运行时抽象。
package console

import "os"

// Line 打印一条消息；style 为空或未知时不加颜色。
func Line(msg string, style ...string) {
	writeLine(os.Stdout, msg, firstStyle(style...), ResolveOutputOptions(os.Stdout, false, false, false, false))
}

// Info 打印一条普通信息消息，使用 Laravel/Symfony info 绿色样式。
func Info(msg string) {
	writeLine(os.Stdout, msg, consoleStyleInfo, ResolveOutputOptions(os.Stdout, false, false, false, false))
}

// Comment 打印一条注释消息，使用 Laravel/Symfony comment 黄色样式。
func Comment(msg string) {
	writeLine(os.Stdout, msg, consoleStyleComment, ResolveOutputOptions(os.Stdout, false, false, false, false))
}

// Question 打印一条问题消息，使用 Laravel/Symfony question 黑字青底样式。
func Question(msg string) {
	writeLine(os.Stdout, msg, consoleStyleQuestion, ResolveOutputOptions(os.Stdout, false, false, false, false))
}

// Success 打印一条成功消息，使用 Prismgo 兼容 Laravel components 的白字绿底样式。
func Success(msg string) {
	writeLine(os.Stdout, msg, consoleStyleSuccess, ResolveOutputOptions(os.Stdout, false, false, false, false))
}

// Warn 打印一条警告消息，使用 Laravel/Symfony warning 黄色样式。
func Warn(msg string) {
	writeLine(os.Stdout, msg, consoleStyleWarn, ResolveOutputOptions(os.Stdout, false, false, false, false))
}

// Error 打印一条报错消息，使用 Laravel/Symfony error 白字红底样式。
func Error(msg string) {
	writeLine(os.Stdout, msg, consoleStyleError, ResolveOutputOptions(os.Stdout, false, false, false, false))
}

// Alert 打印一条 Laravel 风格的黄色警示块。
func Alert(msg string) {
	writeAlert(os.Stdout, msg, ResolveOutputOptions(os.Stdout, false, false, false, false))
}

// Exit 打印一条报错消息，并退出进程。
func Exit(msg string) {
	Error(msg)
	os.Exit(1)
}

// ExitIf 在 err 非空时打印错误并退出进程。
func ExitIf(err error) {
	if err != nil {
		Exit(err.Error())
	}
}
