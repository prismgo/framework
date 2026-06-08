package encryption

import (
	"strings"

	configpkg "github.com/prismgo/framework/config"
)

// NewFromConfig 从配置仓库构造默认应用加密器。
//
// 参数说明：repo 是调用方显式传入的配置仓库；为 nil 时通过 config facade 解析当前
// Application 的默认配置。设计原因是 ServiceProvider 的 lazy factory 不应在 Register
// 阶段读取 APP_KEY，而是在首次解析服务时才读取并校验配置。
func NewFromConfig(repo *configpkg.Config) (*Encrypter, error) {
	if repo == nil {
		repo = configpkg.Resolve()
	}
	return New(Config{
		Key:          repo.GetString("app.key"),
		Cipher:       repo.GetString("app.cipher"),
		PreviousKeys: splitPreviousKeys(repo.GetString("app.previous_keys")),
	})
}

// splitPreviousKeys 解析 APP_PREVIOUS_KEYS 的逗号分隔文本。
//
// value 来自配置项 app.previous_keys；空白条目会被跳过，非空条目只去除逗号分隔时常见的
// 首尾空白，再交给 New/parseKeys 做统一格式校验，避免配置解析层吞掉无效 key。
func splitPreviousKeys(value string) []string {
	parts := strings.Split(value, ",")
	keys := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed == "" {
			continue
		}
		keys = append(keys, trimmed)
	}
	return keys
}
