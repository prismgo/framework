package routine

import (
	"context"
	"errors"
	"fmt"

	contract "github.com/prismgo/framework/contracts/routine"
	goexception "github.com/prismgo/framework/exception"
	"github.com/prismgo/framework/internal/stackx"
)

var errNilTask = errors.New("routine: task is nil")

// Task 创建一次待启动的安全协程配置。
func Task(ctx context.Context, task contract.Task) contract.Builder {
	return &builder{ctx: ctx, task: task}
}

// Go 使用默认配置立即启动安全协程。
func Go(ctx context.Context, task contract.Task) {
	Task(ctx, task).Go()
}

type builder struct {
	ctx       context.Context
	task      contract.Task
	name      string
	component string
	fields    map[string]any
	onPanic   func(error)
	onError   func(error)
}

func (b *builder) Name(name string) contract.Builder {
	b.name = name
	return b
}

func (b *builder) Component(component string) contract.Builder {
	b.component = component
	return b
}

func (b *builder) Fields(fields map[string]any) contract.Builder {
	b.fields = fields
	return b
}

func (b *builder) OnPanic(callback func(error)) contract.Builder {
	b.onPanic = callback
	return b
}

func (b *builder) OnError(callback func(error)) contract.Builder {
	b.onError = callback
	return b
}

func (b *builder) Go() {
	cfg := b.snapshot()
	go cfg.run()
}

func (b *builder) snapshot() taskConfig {
	return taskConfig{
		ctx:       b.ctx,
		task:      b.task,
		name:      b.name,
		component: b.component,
		fields:    b.fields,
		onPanic:   b.onPanic,
		onError:   b.onError,
	}
}

type taskConfig struct {
	ctx       context.Context
	task      contract.Task
	name      string
	component string
	fields    map[string]any
	onPanic   func(error)
	onError   func(error)
}

func (c taskConfig) run() {
	ctx := c.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	defer func() {
		if rec := recover(); rec != nil {
			// 捕获 panic 发生位置的结构化堆栈
			stack := stackx.Capture(0)
			err := c.panicError(rec)
			err = goexception.WithStackTrace(err, stack)
			goexception.Report(ctx, err, c.reportFields())
			if c.onPanic != nil {
				c.onPanic(err)
			}
		}
	}()

	if c.task == nil {
		c.reportError(ctx, errNilTask)
		return
	}
	if err := c.task(ctx); err != nil {
		c.reportError(ctx, err)
	}
}

func (c taskConfig) reportError(ctx context.Context, err error) {
	goexception.Report(ctx, err, c.reportFields())
	if c.onError != nil {
		c.onError(err)
	}
}

func (c taskConfig) panicError(rec any) error {
	if err, ok := rec.(error); ok {
		return err
	}
	return errors.New(fmt.Sprint(rec))
}

func (c taskConfig) reportFields() map[string]any {
	if c.fields == nil && c.name == "" && c.component == "" {
		return nil
	}
	out := make(map[string]any, len(c.fields)+2)
	for key, value := range c.fields {
		out[key] = value
	}
	if c.name != "" {
		out["routine"] = c.name
	}
	if c.component != "" {
		out["component"] = c.component
	}
	return out
}
