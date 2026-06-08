package provider

import (
	"github.com/prismgo/framework/provider/publish"
)

// Publishes 注册可发布资源。
//
// 对齐 Laravel: $this->publishes([source => target], 'tag')
// 在 ServiceProvider.Boot() 中调用。
//
// providerName 是调用方传入的 provider 简短标识（如 "translation"、"acme"），
// 用于 vendor:publish --provider 过滤。调用方应使用 ServiceProvider.Name() 的返回值。
//
// paths 的 key 为相对于调用方源文件的资源路径（如 "lang"），
// 内部自动通过 runtime.Caller 解析调用方源文件目录并转为绝对路径。
// value 为应用项目的绝对目标路径。
//
// 生产环境（support.IsProduction() == true）下本函数静默跳过。
func Publishes(providerName string, paths map[string]string, tags ...string) error {
	return publish.Register(providerName, paths, tags...)
}

// PublishEntries 返回匹配条件的可发布资源条目。
func PublishEntries(provider string, tags []string) []publish.Entry {
	return publish.Entries(provider, tags)
}

// PublishProviders 返回所有已注册可发布资源的 provider 名称。
func PublishProviders() []string {
	return publish.Providers()
}

// PublishTags 返回所有已注册的 tag 名称。
func PublishTags() []string {
	return publish.Tags()
}

// PublishIsAvailable 返回 publish 功能在当前环境下是否可用。
func PublishIsAvailable() bool {
	return publish.IsAvailable()
}

// PublishCopy 执行资源复制（供命令使用）。
func PublishCopy(provider string, tags []string, force bool, existing bool) (published, skipped int, err error) {
	return publish.Copy(provider, tags, force, existing)
}

// PublishDryRun 返回将要发布的文件列表（供命令使用）。
func PublishDryRun(provider string, tags []string) ([]publish.DryRunItem, error) {
	return publish.DryRun(provider, tags)
}

// PublishClear 清空发布注册表（用于测试）。
func PublishClear() {
	publish.Clear()
}
