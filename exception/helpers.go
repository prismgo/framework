package exception

import (
	"strings"

	"github.com/gin-gonic/gin"
)

const (
	// panicLoggedKey 在 gin.Context 中标记 panic 已被异常处理中间件记录，
	// 防止 Gin 内置 Recovery 中间件或其他 panic 捕获点重复写入日志。
	panicLoggedKey = "_panic_logged"

	requestIDContextKey = "request_id"
)

func requestIDFromContext(c *gin.Context) string {
	if c == nil {
		return ""
	}
	value, ok := c.Get(requestIDContextKey)
	if !ok {
		return ""
	}
	id, ok := value.(string)
	if !ok {
		return ""
	}
	return id
}

// joinContextErrors 把 gin.Context 中累积的所有错误合并为一条日志字段。
//
// 设计背景：业务代码或中间件可能通过 c.Error() 追加多层错误
// （如参数验证失败 → 业务规则校验失败），合并后便于日志检索。
// 使用 " | " 分隔各错误消息。
func joinContextErrors(errors []*gin.Error) string {
	messages := make([]string, 0, len(errors))
	for _, current := range errors {
		if current == nil || current.Err == nil {
			continue
		}
		messages = append(messages, current.Err.Error())
	}
	return strings.Join(messages, " | ")
}
