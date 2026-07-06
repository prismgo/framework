// Package runtimex 提供运行时的辅助函数。
package runtimex

import (
	"bytes"
	"log"
	"runtime"
	"strconv"
)

// GoroutineID 返回当前 goroutine 的唯一标识。
//
// 通过解析 runtime.Stack 输出提取 goroutine ID。解析失败时记录警告并返回 0，
// 调用方应将 0 视为未知/不可靠标识。
func GoroutineID() uint64 {
	// 使用 256 字节缓冲区，避免极端情况下 goroutine ID 过大导致截断
	var buf [256]byte
	n := runtime.Stack(buf[:], false)
	if n < 10 {
		log.Printf("WARNING: runtimex.GoroutineID: runtime.Stack returned short header (%d bytes)", n)
		return 0
	}
	// 格式: "goroutine 42 [running]:..."
	// 跳过 "goroutine " (10 bytes)
	rest := buf[10:n]
	idx := bytes.IndexByte(rest, ' ')
	if idx < 0 {
		log.Printf("WARNING: runtimex.GoroutineID: cannot parse goroutine header: %q", string(rest))
		return 0
	}
	id, err := strconv.ParseUint(string(rest[:idx]), 10, 64)
	if err != nil {
		log.Printf("WARNING: runtimex.GoroutineID: bad goroutine ID %q: %v", string(rest[:idx]), err)
		return 0
	}
	return id
}
