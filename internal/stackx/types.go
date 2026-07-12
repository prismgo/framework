package stackx

import (
	"runtime"
	"strconv"
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
	return f.Function + "\n\t" + f.File + ":" + strconv.Itoa(f.Line)
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

// Format 格式化为字符串（延迟解析），默认限制最大 4096 字节。
// 采用"帧对齐截断"策略：写入每个完整帧前预计算长度，超限则跳过该帧直接截断。
// 此策略保证输出中每个帧都是完整的，天然避免无效 UTF-8 序列问题。
func (st *StackTrace) Format() string {
	const defaultMaxBytes = 4096

	if st == nil {
		return ""
	}
	frames := st.Frames()
	if len(frames) == 0 {
		return ""
	}

	var sb strings.Builder
	truncationMarker := "\n... (truncated)"

	for i, frame := range frames {
		frameStr := frame.String()
		// 计算写入此帧后总字节数（非首帧前需加换行符）
		neededLen := len(frameStr)
		if i > 0 {
			neededLen++ // 帧间换行符
		}
		if sb.Len()+neededLen > defaultMaxBytes {
			// 该帧无法完整放入限制内，写入截断标记并返回
			sb.WriteString(truncationMarker)
			return sb.String()
		}
		if i > 0 {
			sb.WriteString("\n")
		}
		sb.WriteString(frameStr)
	}

	return sb.String()
}

// Filter 过滤堆栈帧，返回新的 StackTrace。
// 如果 filter 为 nil，使用 DefaultFilter；否则使用传入的过滤函数。
// 如果没有帧被过滤（全部保留），返回自身避免不必要的分配。
func (st *StackTrace) Filter(fn func(StackFrame) bool) *StackTrace {
	if st == nil {
		return nil
	}

	if fn == nil {
		fn = DefaultFilter()
	}

	frames := st.Frames()
	filtered := make([]StackFrame, 0, len(frames))
	allPassed := true
	for _, frame := range frames {
		if fn(frame) {
			filtered = append(filtered, frame)
		} else {
			allPassed = false
		}
	}
	if allPassed {
		return st
	}
	return &StackTrace{
		frames: filtered,
	}
}

// NewStackTraceFromFrames 从预定义的帧数组构造 StackTrace，
// 用于测试或已知堆栈内容的场景（跳过 runtime.Callers 解析）。
func NewStackTraceFromFrames(frames []StackFrame) *StackTrace {
	return &StackTrace{frames: frames}
}

// FirstLocation 返回第一个 .go 文件的位置信息。
// 跳过非 .go 文件（如 .s 汇编文件），返回 (file, line)。
// 如果没有找到 .go 文件，返回 ("", 0)。
func (st *StackTrace) FirstLocation() (string, int) {
	if st == nil {
		return "", 0
	}
	frames := st.Frames()
	for _, frame := range frames {
		if strings.HasSuffix(frame.File, ".go") {
			return frame.File, frame.Line
		}
	}
	return "", 0
}

// Lines 返回堆栈的行数组表示。
// 每个帧生成两行：函数名和文件:行号。
// 例如：["main.test", "/path/to/file.go:42", "main.caller", "/path/to/caller.go:10"]
//
// 行为说明：
//   - 当接收者为 nil 时返回 nil。
//   - 当堆栈为空（无帧）时返回 nil。
//   - 当堆栈帧全部被过滤后返回非空数组（每个保留帧生成两行）。
func (st *StackTrace) Lines() []string {
	if st == nil {
		return nil
	}
	frames := st.Frames()
	if len(frames) == 0 {
		return nil
	}

	lines := make([]string, 0, len(frames)*2)
	for _, frame := range frames {
		lines = append(lines, frame.Function)
		lines = append(lines, frame.File+":"+strconv.Itoa(frame.Line))
	}
	return lines
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
