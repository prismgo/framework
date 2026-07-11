package stackx

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
)

// StackFrame 表示单个堆栈帧
type StackFrame struct {
	Function string  `json:"function"` // 函数名
	File     string  `json:"file"`     // 文件路径
	Line     int     `json:"line"`     // 行号
	PC       uintptr `json:"-"`        // 程序计数器，用于延迟解析
}

// String 返回堆栈帧的字符串表示
func (f StackFrame) String() string {
	return fmt.Sprintf("%s\n\t%s:%d", f.Function, f.File, f.Line)
}

// StackTrace 延迟解析的堆栈
type StackTrace struct {
	pcs    []uintptr    // 原始 PC 指针
	frames []StackFrame // 延迟解析后的帧
	once   sync.Once    // 确保只解析一次
}

// Frames 返回解析后的帧数组
// 首次调用时会解析 PC 指针为 StackFrame
// 如果 frames 已被设置（例如通过 Filter），直接返回已设置的值
func (st *StackTrace) Frames() []StackFrame {
	if st == nil {
		return nil
	}
	// 如果 frames 已经被设置（例如通过 Filter），直接返回
	if st.frames != nil {
		return st.frames
	}
	st.once.Do(func() {
		st.frames = resolveFrames(st.pcs)
	})
	return st.frames
}

// Format 格式化为字符串（延迟解析）
func (st *StackTrace) Format() string {
	if st == nil {
		return ""
	}
	frames := st.Frames()
	if len(frames) == 0 {
		return ""
	}
	
	var sb strings.Builder
	for i, frame := range frames {
		if i > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(frame.String())
	}
	return sb.String()
}

// Filter 过滤堆栈帧，返回新的 StackTrace
func (st *StackTrace) Filter(fn func(StackFrame) bool) *StackTrace {
	if st == nil {
		return nil
	}
	frames := st.Frames()
	filtered := make([]StackFrame, 0, len(frames))
	for _, frame := range frames {
		if fn(frame) {
			filtered = append(filtered, frame)
		}
	}
	return &StackTrace{
		frames: filtered,
	}
}

// resolveFrames 将 PC 指针解析为 StackFrame
func resolveFrames(pcs []uintptr) []StackFrame {
	if len(pcs) == 0 {
		return nil
	}
	
	frames := make([]StackFrame, 0, len(pcs))
	callersFrames := runtime.CallersFrames(pcs)
	
	for {
		frame, more := callersFrames.Next()
		frames = append(frames, StackFrame{
			Function: frame.Function,
			File:     frame.File,
			Line:     frame.Line,
			PC:       frame.PC,
		})
		if !more {
			break
		}
	}
	
	return frames
}
