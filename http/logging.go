package http

import (
	"github.com/gin-gonic/gin"
)

// IdentityExtractor 从 gin.Context 中提取业务身份字段（如 tenant_id / user_id / role），
// 供请求日志附加。返回空 map 表示不附加任何身份字段。
type IdentityExtractor func(c *gin.Context) map[string]any
