package queue

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prismgo/framework/console"
	queuecore "github.com/prismgo/framework/queue"
)

// resolveManager 优先解析应用注册的队列 manager；解析失败时显式返回错误。
//
// 设计说明：queue 管理类命令若静默回退到默认 sync manager，可能误操作错误后端，
// 因此这里把初始化错误暴露给调用方，由命令层决定是否继续执行。
func resolveManager() (*queuecore.Manager, error) {
	manager := queuecore.Resolve()
	if manager == nil {
		return nil, fmt.Errorf("resolve queue manager failed: queue manager not initialized")
	}
	return manager, nil
}

// splitQueueNames 把逗号分隔的队列名转换为 worker 可消费的有序列表。
func splitQueueNames(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if name := strings.TrimSpace(part); name != "" {
			result = append(result, name)
		}
	}
	return result
}

// secondsOption 读取秒级命令选项，并转换为 time.Duration。
func secondsOption(ctx console.CommandContext, name string, fallback int) time.Duration {
	return time.Duration(intOption(ctx, name, fallback)) * time.Second
}

// intOption 读取整数命令选项；非法输入保持 Laravel 风格的默认值兜底。
func intOption(ctx console.CommandContext, name string, fallback int) int {
	value := strings.TrimSpace(ctx.Input().Option(name))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	return n
}

// parseBackoff 解析 "1,5,10" 形式的重试退避秒数。
func parseBackoff(value string) []time.Duration {
	value = strings.TrimSpace(value)
	if value == "" || value == "0" {
		return nil
	}
	parts := strings.Split(value, ",")
	result := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err == nil && n > 0 {
			result = append(result, time.Duration(n)*time.Second)
		}
	}
	return result
}
