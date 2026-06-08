package horizon

import (
	"strconv"
	"strings"
)

const (
	defaultPageNumber = 1
	defaultPageSize   = 50
	maxPageSize       = 100
)

// PageRequest 描述 Horizon 只读列表的分页请求。
//
// 设计思路：所有列表接口统一使用 page/page_size，便于 Dashboard 在首屏和 tab
// 刷新时复用同一套参数解析逻辑。
type PageRequest struct {
	Page     int
	PageSize int
}

// PageEnvelope 是 Horizon 只读分页响应的通用外壳。
type PageEnvelope[T any] struct {
	Items      []T    `json:"items"`
	Total      int    `json:"total"`
	Page       int    `json:"page"`
	PageSize   int    `json:"page_size"`
	Capability string `json:"capability,omitempty"`
	Reason     string `json:"reason,omitempty"`
}

// parsePageRequest 解析 page/page_size，并把非法值回退到稳定默认值。
func parsePageRequest(pageValue, sizeValue string) PageRequest {
	request := PageRequest{Page: defaultPageNumber, PageSize: defaultPageSize}
	if page := parsePositiveInt(pageValue); page > 0 {
		request.Page = page
	}
	if size := parsePositiveInt(sizeValue); size > 0 && size <= maxPageSize {
		request.PageSize = size
	}
	return request
}

func parsePositiveInt(value string) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	n, err := strconv.Atoi(value)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}

func pageSlice[T any](items []T, request PageRequest) []T {
	start := pageStart(request)
	if start >= len(items) {
		return []T{}
	}
	end := start + request.PageSize
	if end > len(items) {
		end = len(items)
	}
	return append([]T(nil), items[start:end]...)
}

func pageStart(request PageRequest) int {
	if request.Page <= 1 {
		return 0
	}
	return (request.Page - 1) * request.PageSize
}
