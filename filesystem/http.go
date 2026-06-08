package filesystem

import "net/url"

// urlPathEscape 对路径片段做 URL 安全转义。
func urlPathEscape(value string) string {
	return url.PathEscape(value)
}

// urlQueryEscape 对查询字符串值做 URL 安全转义。
func urlQueryEscape(value string) string {
	return url.QueryEscape(value)
}
