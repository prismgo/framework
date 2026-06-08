package queue

import (
	"fmt"
	"reflect"
	"sync"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	encodingpkg "github.com/prismgo/framework/encoding"
	"github.com/prismgo/framework/queue/payload"
)

// Registry 保存任务类型名与反序列化工厂之间的映射。
//
// durable queue 保存的是当前 queue Payload Encoding 编码后的 payload，Go 运行时不能从字符串
// 直接创建类型，因此独立 worker 必须在启动时通过 RegisterType 准备好类型表。
type Registry struct {
	mu        sync.RWMutex
	factories map[string]func() Job
}

// NewRegistry 创建空任务注册表。
func NewRegistry() *Registry {
	return &Registry{factories: make(map[string]func() Job)}
}

// JobTypeName 返回任务类型的稳定队列标识。
//
// 标识格式为完整 Go 类型名，例如 prismgo/app/jobs.SendWelcomeMailJob。
func JobTypeName(job Job) (string, error) {
	if job == nil {
		return "", fmt.Errorf("queue registry: job is nil")
	}
	return jobTypeName(reflect.TypeOf(job))
}

// RegisterType 把任务类型注册到全局注册表。
//
// 业务 Job 通常在所在包的 init 中调用：
//
//	func init() { queue.RegisterType[*SendWelcomeMailJob]() }
func RegisterType[T Job]() {
	RegisterTypeTo[T](defaultRegistry)
}

// RegisterTypeTo 把任务类型注册到指定注册表，主要用于测试或独立 manager。
func RegisterTypeTo[T Job](registry *Registry) {
	if registry == nil {
		return
	}
	var zero T
	typ := reflect.TypeOf(zero)
	name, err := jobTypeName(typ)
	if err != nil {
		return
	}
	registry.registerFactory(name, func() Job {
		return newJobFromType(typ)
	})
}

// RegisterJobType 把当前任务实例的具体类型注册到当前注册表。
func (r *Registry) RegisterJobType(job Job) {
	r.registerJobType(job)
}

// registerJobType 把当前任务实例的具体类型注册到当前注册表。
//
// Dispatch 会自动调用该方法，以便同进程 sync 或测试场景不需要额外注册。
// 独立 worker 仍需要显式 RegisterType，因为 worker 进程看不到 dispatch 进程的内存表。
func (r *Registry) registerJobType(job Job) {
	if r == nil || job == nil {
		return
	}
	typ := reflect.TypeOf(job)
	name, err := jobTypeName(typ)
	if err != nil {
		return
	}
	r.registerFactory(name, func() Job {
		return newJobFromType(typ)
	})
}

// Marshal 把任务编码成队列 payload。
//
// 兼容说明：公开方法保留历史 JSON 语义；Manager 内部会调用 marshalWithCodec 使用当前
// queue Payload Encoding，避免新增公共 Encoding 方法或强迫调用方感知 codec。
func (r *Registry) Marshal(job Job) (payload.Payload, error) {
	return r.marshalWithCodec(job, encodingpkg.JSON())
}

func (r *Registry) marshalWithCodec(job Job, codec encodingcontract.Codec) (payload.Payload, error) {
	if job == nil {
		return nil, fmt.Errorf("queue registry: job is nil")
	}
	if codec == nil {
		codec = encodingpkg.Msgpack()
	}
	body, err := codec.Marshal(job)
	if err != nil {
		name, nameErr := JobTypeName(job)
		if nameErr != nil {
			name = "<unknown>"
		}
		return nil, fmt.Errorf("queue registry: marshal %s: %w", name, err)
	}
	return payload.Payload(body), nil
}

// Unmarshal 按任务类型名恢复具体 Job。
//
// 兼容说明：公开方法保留历史 JSON 语义；worker/job runner 内部会调用 unmarshalWithCodec 使用
// Manager 持有的 queue Payload Encoding。
func (r *Registry) Unmarshal(name string, payload payload.Payload) (Job, error) {
	return r.unmarshalWithCodec(name, payload, encodingpkg.JSON())
}

func (r *Registry) unmarshalWithCodec(name string, payload payload.Payload, codec encodingcontract.Codec) (Job, error) {
	if r == nil {
		return nil, fmt.Errorf("%w: %s", ErrJobNotRegistered, name)
	}
	r.mu.RLock()
	factory := r.factories[name]
	r.mu.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("%w: %s", ErrJobNotRegistered, name)
	}
	job := factory()
	if job == nil {
		return nil, fmt.Errorf("queue registry: factory for %s returned nil", name)
	}
	if len(payload) == 0 {
		return job, nil
	}
	if codec == nil {
		codec = encodingpkg.Msgpack()
	}
	if err := codec.Unmarshal(payload, job); err != nil {
		return nil, fmt.Errorf("queue registry: unmarshal %s: %w", name, err)
	}
	return job, nil
}

// Has 判断任务类型名是否已经注册。
func (r *Registry) Has(name string) bool {
	if r == nil {
		return false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.factories[name] != nil
}

func jobTypeName(typ reflect.Type) (string, error) {
	if typ == nil {
		return "", fmt.Errorf("queue registry: job type is nil")
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	if typ.PkgPath() == "" || typ.Name() == "" {
		return "", fmt.Errorf("queue registry: unsupported job type %s", typ.String())
	}
	return typ.PkgPath() + "." + typ.Name(), nil
}

func newJobFromType(typ reflect.Type) Job {
	if typ == nil {
		return nil
	}
	for typ.Kind() == reflect.Pointer {
		typ = typ.Elem()
	}
	value := reflect.New(typ)
	job, _ := value.Interface().(Job)
	return job
}

func (r *Registry) registerFactory(name string, factory func() Job) {
	if r == nil || name == "" || factory == nil {
		return
	}
	r.mu.Lock()
	r.factories[name] = factory
	r.mu.Unlock()
}
