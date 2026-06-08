// Package state 保存 queue 查询型状态的通用 DTO。
package state

// PageRequest 描述只读分页请求。
//
// 语义说明：Page 从 1 开始；PageSize 小于等于 0 时由具体 Store 使用自己的稳定默认值。
type PageRequest struct {
	Page     int
	PageSize int
}

// PageEnvelope 是只读分页响应的通用外壳。
//
// Total 表示当前过滤条件下的完整数量，不只是 Items 的长度；调用方可用它计算是否还有下一页。
type PageEnvelope[T any] struct {
	Items    []T `json:"items"`
	Total    int `json:"total"`
	Page     int `json:"page"`
	PageSize int `json:"page_size"`
}

// MaxPageSize 限制单次分页请求的最大条目数。
//
// 设计原因：防止调用方传入极大 PageSize 导致内存分配过多或查询超时。
// 超过此值会被截断到 MaxPageSize。
const MaxPageSize = 1000

// NormalizePage 归一化 queue state 分页默认值。
//
// 逻辑说明：Page 从 1 开始；PageSize 小于等于 0 时使用默认值 50；
// PageSize 超过 MaxPageSize 时截断到上限，防止单次请求返回过多数据。
func NormalizePage(page PageRequest) PageRequest {
	if page.Page <= 0 {
		page.Page = 1
	}
	if page.PageSize <= 0 {
		page.PageSize = 50
	}
	if page.PageSize > MaxPageSize {
		page.PageSize = MaxPageSize
	}
	return page
}

// PageSlice 返回当前页切片副本。
func PageSlice[T any](items []T, page PageRequest) []T {
	start := 0
	if page.Page > 1 {
		start = (page.Page - 1) * page.PageSize
	}
	if start >= len(items) {
		return []T{}
	}
	end := start + page.PageSize
	if end > len(items) {
		end = len(items)
	}
	return append([]T(nil), items[start:end]...)
}
