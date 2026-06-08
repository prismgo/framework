package http

import (
	"strings"

	"github.com/gin-gonic/gin"
)

const panicLoggedKey = "_panic_logged"

// IdentityExtractor 从 gin.Context 中提取业务身份字段（如 tenant_id / user_id / role），
// 供请求日志附加。返回空 map 表示不附加任何身份字段。
type IdentityExtractor func(c *gin.Context) map[string]any

// joinContextErrors 把 gin.Context 中累计的错误合并成一条日志字段内容。
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
