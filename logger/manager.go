package logger

import (
	"fmt"
	"strings"
	"sync"

	"github.com/prismgo/framework/container"
	"github.com/prismgo/framework/internal/fmtx"
)

// Config 是 NewManager 的输入参数，描述整个日志体系的顶层配置。
// 与 Laravel 的 config/logging.php 语义一致：Default 指向默认通道，
// Channels 给出全部可用通道的定义。
type Config struct {
	Default          string
	Channels         map[string]ChannelOptions
	ContextExtractor ContextExtractor
}

// Manager 管理全部通道的生命周期、查找与 fallback。
// 支持：
//   - 惰性构造：Channel(name) 第一次被访问时再解析驱动；节省启动开销。
//   - 未知通道回退到默认通道；
//   - stack 驱动延迟到首次使用时解析子通道，避免启动时的循环依赖。
type Manager struct {
	mu       sync.Mutex
	defName  string
	specs    map[string]ChannelOptions
	resolved map[string]Logger
	// building 用于检测 stack 驱动嵌套引用自身导致的死循环。
	building map[string]bool
	closed   bool
}

type configReader interface {
	GetString(path string, defaultValue ...any) string
	GetStringMap(path string) map[string]any
}

// NewManager 根据 Config 创建 Manager。
// 不立即构造任何通道（惰性），但会校验 default 名字有效。
func NewManager(opts Config) (*Manager, error) {
	def := strings.TrimSpace(opts.Default)
	if def == "" {
		return nil, fmt.Errorf("logger: default channel is required")
	}
	if _, ok := opts.Channels[def]; !ok {
		return nil, fmt.Errorf("logger: default channel %q not defined", def)
	}
	specs := make(map[string]ChannelOptions, len(opts.Channels))
	for name, c := range opts.Channels {
		if c.ContextExtractor == nil {
			c.ContextExtractor = opts.ContextExtractor
		}
		specs[name] = c
	}
	return &Manager{
		defName:  def,
		specs:    specs,
		resolved: make(map[string]Logger),
		building: make(map[string]bool),
	}, nil
}

// DefaultName 返回默认通道的名称。
func (m *Manager) DefaultName() string { return m.defName }

// Default 返回默认通道对应的 Logger。
func (m *Manager) Default() Logger {
	return m.Channel(m.defName)
}

// Channel 按名称获取通道；未知名称回退到默认通道。
// 首次访问时惰性构造并缓存。
func (m *Manager) Channel(name string) Logger {
	name = strings.TrimSpace(name)
	if name == "" {
		name = m.defName
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resolveLocked(name)
}

// resolveLocked 在持锁状态下解析通道，供 Channel 与 stack 子通道共用。
func (m *Manager) resolveLocked(name string) Logger {
	if m.closed {
		// Manager 关闭后进入终态；后续日志属于生命周期尾部误写，统一丢弃。
		return m.buildNullFallback()
	}
	if lg, ok := m.resolved[name]; ok {
		return lg
	}
	spec, ok := m.specs[name]
	if !ok {
		// 未知通道直接回退到 default，避免业务代码因为一次漂移而 panic。
		if name == m.defName {
			// 极端场景：default 本身未定义，NewManager 已拦截；这里返回一个丢弃型通道保底。
			lg := m.buildNullFallback()
			m.resolved[name] = lg
			return lg
		}
		return m.resolveLocked(m.defName)
	}
	if m.building[name] {
		// stack 嵌套引用自身时返回 null，避免死锁。
		return m.buildNullFallback()
	}
	m.building[name] = true
	defer delete(m.building, name)

	lg, err := m.buildLocked(name, spec)
	if err != nil {
		// 构造失败：记录后回退到默认通道（除非自己就是默认通道，此时返回 null 避免递归）。
		fmtx.Fprintf(errWriter(), "logger: build channel %q failed: %v\n", name, err)
		if name == m.defName {
			lg = m.buildNullFallback()
		} else {
			lg = m.resolveLocked(m.defName)
		}
	}
	m.resolved[name] = lg
	return lg
}

// buildLocked 按 driver 分派构造逻辑。stack 需要递归解析子通道。
func (m *Manager) buildLocked(name string, spec ChannelOptions) (Logger, error) {
	driver := strings.ToLower(strings.TrimSpace(spec.Driver))
	if driver == "stack" {
		if len(spec.Channels) == 0 {
			return nil, fmt.Errorf("stack channel %q requires channels list", name)
		}
		children := make([]Logger, 0, len(spec.Channels))
		for _, child := range spec.Channels {
			child = strings.TrimSpace(child)
			if child == "" || child == name {
				continue
			}
			children = append(children, m.resolveLocked(child))
		}
		if len(children) == 0 {
			return nil, fmt.Errorf("stack channel %q has no valid children", name)
		}
		return newStackLogger(children, m), nil
	}
	return newChannel(name, spec, m)
}

// buildNullFallback 返回一个绑定 null 驱动的通道，作为最后兜底。
func (m *Manager) buildNullFallback() Logger {
	c, _ := newChannel("__null__", ChannelOptions{Driver: "null", Level: "error"}, m)
	return c
}

// NewManagerFromConfig 根据当前 config facade 创建日志 Manager。
//
// 需求背景：provider 的 lazy factory 需要复用同一套配置解析和 manager 构造逻辑，
// 但不再暴露旧的 Application 装配命名，避免调用方误以为这是启动注册入口。
func NewManagerFromConfig(...any) (func() error, *Manager, error) {
	cfg, err := buildConfig()
	if err != nil {
		return nil, nil, err
	}
	m, err := NewManager(cfg)
	if err != nil {
		return nil, nil, err
	}
	return m.Close, m, nil
}

// buildConfig 从当前 Application config facade 严格构造日志配置。
//
// 需求背景：logger provider 的 Register 阶段只注册 lazy factory；真正读取配置发生在
// logger.Resolve()，此时必须把缺失 config 或 logging.channels 的错误返回给调用方。
func buildConfig() (Config, error) {
	cfg, err := container.Make[configReader]("config.default")
	if err != nil {
		return Config{}, err
	}
	def := cfg.GetString("logging.default", "stack")
	rawChannels := cfg.GetStringMap("logging.channels")
	if len(rawChannels) == 0 {
		return Config{}, fmt.Errorf("logging.channels is empty")
	}

	channels := make(map[string]ChannelOptions, len(rawChannels))
	for name, raw := range rawChannels {
		spec, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		channels[name] = ChannelOptions{
			Driver:    castString(spec["driver"]),
			Formatter: castString(spec["formatter"]),
			Level:     castString(spec["level"]),
			Path:      castString(spec["path"]),
			Channels:  castStringSlice(spec["channels"]),
		}
	}
	return Config{Default: def, Channels: channels}, nil
}

func castString(v any) string {
	if v == nil {
		return ""
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func castStringSlice(v any) []string {
	if v == nil {
		return nil
	}
	switch items := v.(type) {
	case []string:
		out := make([]string, 0, len(items))
		for _, s := range items {
			if s != "" {
				out = append(out, s)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(items))
		for _, item := range items {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	}
	return nil
}

// Close 释放所有已构造通道的底层驱动。
// 由 bootstrap/logger.go 在进程退出前调用。
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var firstErr error
	for _, lg := range m.resolved {
		c, ok := lg.(*channel)
		if !ok {
			continue
		}
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.resolved = make(map[string]Logger)
	m.closed = true
	return firstErr
}
