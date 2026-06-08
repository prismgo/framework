package optional

// Bool 表示可选布尔值，用于区分“未配置”和“显式配置为 false”。
//
// 需求背景：
// Go 的 bool 零值无法表达调用方是否真的传入了 false。队列、缓存等通用配置在合并默认值时，
// 需要保留显式 false，同时让未配置字段继承默认值。
type Bool struct {
	set   bool
	value bool
}

// NewBool 创建一个已设置的可选布尔值。
func NewBool(value bool) Bool {
	return Bool{set: true, value: value}
}

// IsSet 返回调用方是否显式设置过该布尔值。
func (b Bool) IsSet() bool {
	return b.set
}

// Or 在未设置时返回 fallback；已设置时返回显式值。
func (b Bool) Or(fallback bool) bool {
	if b.set {
		return b.value
	}
	return fallback
}
