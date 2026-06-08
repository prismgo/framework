package queue

import (
	"context"
	"fmt"
	"sync"

	encodingcontract "github.com/prismgo/framework/contracts/encoding"
	encryptioncontract "github.com/prismgo/framework/contracts/encryption"
	queuecontract "github.com/prismgo/framework/contracts/queue"
	encryptionpkg "github.com/prismgo/framework/encryption"
	"github.com/prismgo/framework/queue/payload"
)

// Runtime 承载队列运行期依赖，和 Laravel QueueManager 的连接解析职责分离。
//
// 需求背景：Manager 只应该像 Laravel QueueManager 一样缓存连接、解析 connector 和连接配置；
// registry、payload codec、middleware、failed/batch/restart state 都属于 dispatcher/worker
// 运行期编排。集中到 Runtime 后，新增运行期能力不会继续塞回 Manager。
type Runtime struct {
	defaultConnection string
	defaultQueue      string
	cacheDriver       string
	failed            FailedStore
	batch             BatchStore
	restart           queuecontract.RestartStore
	restartOnce       sync.Once
	registry          *Registry
	codec             encodingcontract.Codec
	middlewareMu      sync.RWMutex
	middleware        []Middleware
	payloadEncrypter  encryptioncontract.Encrypter
}

// UseMiddleware 注册运行期任务 middleware。
//
// 参数 middleware 是追加到所有 JobRunner 的全局 middleware 链，按注册顺序执行。
func (r *Runtime) UseMiddleware(middleware ...Middleware) {
	if r == nil || len(middleware) == 0 {
		return
	}
	r.middlewareMu.Lock()
	r.middleware = append(r.middleware, middleware...)
	r.middlewareMu.Unlock()
}

// middlewareSnapshot 返回 middleware 链副本，避免 worker 执行期间被并发注册影响。
func (r *Runtime) middlewareSnapshot() []Middleware {
	if r == nil {
		return nil
	}
	r.middlewareMu.RLock()
	defer r.middlewareMu.RUnlock()
	return append([]Middleware(nil), r.middleware...)
}

// encryptPayload 对 ShouldEncrypt/Encrypt job 的业务 payload 加密。
//
// 参数 body 是已经由 Registry 编码出的 Job payload；返回值是可写入 Envelope.Payload 的加密
// raw message。加密能力属于 runtime payload factory 边界，不属于 Manager 连接解析职责。
func (r *Runtime) encryptPayload(body payload.Payload) (payload.Payload, error) {
	encrypter, err := r.resolvePayloadEncrypter()
	if err != nil {
		return nil, err
	}
	token, err := encrypter.Encrypt(context.Background(), body)
	if err != nil {
		return nil, err
	}
	return encryptedRawMessage(string(token))
}

func (r *Runtime) resolvePayloadEncrypter() (encryptioncontract.Encrypter, error) {
	if r == nil {
		return nil, fmt.Errorf("queue: payload encrypter is not configured")
	}
	if r.payloadEncrypter != nil {
		return r.payloadEncrypter, nil
	}
	encrypter, err := resolveDefaultPayloadEncrypter()
	if err != nil {
		return nil, err
	}
	return encrypter, nil
}

// resolveDefaultPayloadEncrypter 通过 encryption facade 懒解析默认 byte 加密契约。
//
// 设计原因：queue manager 本身可能只处理明文 Job；只有 ShouldEncrypt/Encrypt 真正请求
// payload 加密时才解析 encryption 服务，避免普通队列启动被未配置 APP_KEY 阻断。
func resolveDefaultPayloadEncrypter() (encrypter encryptioncontract.Encrypter, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if recoveredErr, ok := recovered.(error); ok {
				err = recoveredErr
				return
			}
			err = fmt.Errorf("queue: resolve encryption service: %v", recovered)
		}
	}()
	return encryptionpkg.Resolve(), nil
}
