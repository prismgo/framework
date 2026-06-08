package event

// EventVendorTagPublished vendor:publish 完成资源发布后触发的事件名。
const EventVendorTagPublished = "vendor.tag.published"

// VendorTagPublished vendor:publish 命令完成资源发布后触发的事件。
//
// 需求背景：对齐 Laravel 13 VendorTagPublished 事件，
// 允许扩展包或业务通过监听该事件执行发布后续操作。
//
// 字段说明：
//   - Tag 本次发布的 tag 列表
//   - Published 本次发布新创建的文件数
//   - Skipped 本次因已存在或过滤跳过的文件数
type VendorTagPublished struct {
	Tag       []string
	Published int
	Skipped   int
}

// Name 实现 Event 接口。
func (VendorTagPublished) Name() string { return EventVendorTagPublished }
