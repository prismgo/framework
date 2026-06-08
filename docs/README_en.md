# PrismGo

<div align="center">

**PrismGo - Write Go Like Laravel**

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Module](https://img.shields.io/badge/module-github.com%2Fprismgo%2Fframework-blue)](https://github.com/prismgo/framework)
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE)

[简体中文](../README.md) | English

</div>

---

## What Is It?

PrismGo is a **Laravel-style Go web framework** developed fully by AI agents. It carries Laravel's design philosophy throughout while fitting naturally into mainstream Go coding practices. If you are familiar with Laravel's developer experience - Facades, ServiceProviders, Artisan commands, cache systems, Eloquent-style ORM, events, queued jobs, and logging - you will find the same feel in PrismGo.

We want Go developers to avoid choosing between "high performance" and "high development efficiency." PrismGo uses mature components from the Go ecosystem ([Gin](https://github.com/gin-gonic/gin), [GORM](https://github.com/go-gorm/gorm), [Redis](https://github.com/redis/go-redis), [Viper](https://github.com/spf13/viper), [Logrus](https://github.com/sirupsen/logrus), [Cobra](https://github.com/spf13/cobra)) and organizes them with Laravel's design philosophy into a complete, ready-to-use web toolkit.

> **In one sentence: PrismGo lets you write Go syntax while enjoying Laravel's developer experience.**

---

## Why Choose PrismGo?

| | Raw Gin/GORM | PrismGo |
|---|---|---|
| **Routing** | Hand-written Gin Router setup | `route.Get("/users/{id}")`, named routes, resource routes, groups |
| **Commands** | Raw main/flag code | Artisan-style CLI: `go run . serve` `go run . migrate` |
| **Configuration** | `viper.Get` everywhere | Unified dot-path access: `config.GetString("app.name")` |
| **Logging** | Raw logrus usage | Multi-channel logs: `logger.Channel("error").Error(...)` |
| **Cache** | Wrap Redis yourself | `cache.Remember(ctx, key, ttl, callback)` |
| **Events** | None | `event.Dispatch(ctx, OrderPaid{ID: 1001})` + listener |
| **Queues** | Build workers yourself | `queue.Dispatch(ctx, job)`, Redis/RabbitMQ ready out of the box |
| **Migrations** | Hand-written SQL | Schema Blueprint: `$table->String("name")` |
| **Resource Management** | Each resource closes itself | Unified application lifecycle: boot registration and shutdown cleanup |

The core advantage is simple: **you know what you want to build, and the framework removes the boilerplate.**

---

## Quick Start

### Installation

```bash
go get github.com/prismgo/framework
```

### Minimal Runnable Example

```go
package main

import (
	"context"
	"os"

	"github.com/prismgo/framework/foundation"
	"github.com/prismgo/framework/route"
)

func main() {
	app := foundation.Configure().
		WithRouting(func(r route.Registrar) {
			r.Get("/", func(c *gin.Context) {
				c.JSON(200, gin.H{"message": "Hello PrismGo!"})
			})
		}).
		Create()

	if err := app.HandleCommand(context.Background(), os.Args); err != nil {
		console.Exit(err.Error())
	}
}
```

```bash
go run . serve --port=8000
```

Open `http://localhost:8000` and you will see `{"message": "Hello PrismGo!"}`.

---

## Core Features At a Glance

## 🎭 Facade: Call Services Like Global Functions

```go
cache.Put(ctx, "key", value, ttl)           // Cache
logger.Channel("error").Error("failed")     // Logging
event.Dispatch(ctx, ev)                      // Events
queue.Dispatch(ctx, job)                     // Queue
db := database.Resolve()                    // Database
```

As convenient as Laravel Facades. Under the hood, the Application container manages lifecycles, avoiding global variable pollution and initialization order problems.

---

## 🧩 Provider-Driven Architecture

PrismGo uses the same ServiceProvider pattern as Laravel to organize features:

```go
type CacheServiceProvider struct{}

func (p *CacheServiceProvider) Register(app provider.Application) error {
	app.Container().Singleton("cache.manager", func() (any, error) {
		return cache.NewManager()
	}, container.WithCloser(func(m *Manager) error {
		return m.Close()
	}))
	return nil
}

func (p *CacheServiceProvider) Boot(app provider.Application) error {
	return nil
}
```

Each feature module is registered through a Provider, loaded lazily, and released cleanly. You only need to declare providers in `bootstrap/app.go`:

```go
foundation.Configure().
	WithProviders(
		&cache.ServiceProvider{},
		&queue.ServiceProvider{},
		&filesystem.ServiceProvider{},
	)
```

The framework automatically loads providers in dependency order and releases all resources in reverse order on shutdown.

---

### 🎨 Laravel-Style Routing

```go
route.Prefix("/api").Middleware(auth).Group(func(r route.Registrar) {
	r.Get("/users", userController.Index).Name("users.index")
	r.Post("/users", userController.Store).Name("users.store")
	r.Get("/users/{id}", userController.Show).Name("users.show")
})
```

Named routes, nested groups, middleware, and parameter constraints - written like Laravel Router.

---

### 🖥 Artisan-Style CLI

```bash
go run . serve --port=8051          # Start the HTTP server
go run . migrate                    # Run database migrations
go run . db:seed                    # Seed the database
go run . queue --queue=default      # Start the queue worker
go run . cron                       # Start the scheduler
```

Registering a custom command is as simple as in Laravel. Define a command by implementing only `Definition()` and `Handle()`:

```go
// app/cmd/report_daily.go
type DailyReportCommand struct{}

func (c *DailyReportCommand) Definition() *console.Definition {
	return console.MustDefinition(
		"report:daily {date? : Report date, defaults to today} {--e|email= : Send to the specified email address}",
		"Generate the daily business report",
	)
}

func (c *DailyReportCommand) Handle(ctx console.CommandContext) error {
	date := ctx.Input().Argument("date")
	email := ctx.Input().Option("email")
	// Business logic...
	ctx.IO().Success("Daily report generated: %s", date)
	return nil
}
```

Register it:

```go
// bootstrap/app.go
foundation.Configure().
	WithRouting(func(r *foundation.Routing) {
		r.Commands(
			func() console.Command { return NewDailyReportCommand() },
		)
	})
```

`MustDefinition` accepts an Artisan-style signature DSL and automatically parses argument types, required and optional arguments, short option aliases, with no hand-written Cobra binding code.

---

### ⏱ Scheduled Tasks

```go
schedule.Command("report:daily --force").DailyAt("08:00")
schedule.Command("cache:clean").EveryThirtyMinutes()
```

Start them with the `cron` command. One line of configuration is enough.

---

### 📦 Event System

```go
event.ListenFunc("order.paid", func(ctx context.Context, e event.Event) error {
	order := e.(*OrderPaid)
	notification.Send(ctx, order.UserID, "Your order has been paid")
	return nil
})

event.Dispatch(ctx, &OrderPaid{ID: order.ID})
```

Switch freely among synchronous listeners, async goroutines, and queued listeners.

---

### ⏳ Queue Jobs

Implement just one interface:

```go
type SendEmailJob struct {
	To      string
	Subject string
	Body    string
}

func (j *SendEmailJob) Handle(ctx context.Context) error {
	return mailer.Send(ctx, j.To, j.Subject, j.Body)
}

queue.Dispatch(ctx, &SendEmailJob{
	To: "user@example.com", Subject: "Welcome", Body: "...",
})
```

Retries, timeouts, unique jobs, batches, and chained jobs are supported. Drivers include Redis, RabbitMQ, and Sync.

---

### 🗃 Cache System

```go
cache.Put(ctx, "user:1", user, 10*time.Minute)

user, err := cache.Get[User](ctx, "user:1")

user, err := cache.Remember(ctx, "stats:daily", 1*time.Hour, func() (*Stats, error) {
	return computeStats(ctx)
})
```

Drivers include memory, redis, file, and failover. Advanced features include `Tags`, `Flexible` (stale-while-revalidate), `Lock` (distributed locks), and `Memo` (request-level memoization).

---

### 📁 Filesystem

```go
disk := filesystem.Disk("public")

disk.PutFileAs(ctx, "avatars", uploadedFile, "user_123.jpg")

url := disk.URL("avatars/user_123.jpg")
```

A unified Disk abstraction supports local, public, and Alibaba Cloud OSS. Switching drivers only requires changing configuration.

---

### 📝 Multi-Channel Logging

```go
logger.Channel("daily").WithFields(logrus.Fields{
	"user_id": userID,
}).Info("User logged in successfully")

logger.Channel("error").WithError(err).Error("Order sync failed")
```

Supports stack, single, daily, stderr, and null drivers. Channels are lazily loaded on demand, and construction failures automatically fall back to the default channel so logging system failures do not affect business availability.

---

### 🗄 Database & Schema Migrations

Write migrations with Blueprint instead of hand-written SQL:

```go
schema.Bind(db).Create("orders", func(table *schema.Blueprint) {
	table.ID()
	table.String("order_no", 32).Unique()
	table.UnsignedBigInteger("user_id")
	table.Decimal("amount", 10, 2)
	table.Timestamps()
	table.SoftDeletes()
})
```

GORM is used as the underlying ORM, with an Eloquent-style query API:

```go
db.Where("status = ?", "paid").Order("created_at desc").Find(&orders)
```

---

### 📊 Rate Limiting

```go
ratelimit.For("api").Limit(60).PerMinute()
```

Mount it directly as middleware. It shares cache storage and supports custom over-limit responses.

---

## Component Overview

| Component | What It Does | How to Use |
|---|---|---|
| `foundation` | Application startup, Provider registration, lifecycle, resource closing | `foundation.NewApplication()` |
| `horizon` | Queue monitoring panel, worker management, job metrics, Dashboard | `go run . horizon` |
| `route` | Laravel-style route declarations on top of Gin | `route.Get("/", handler).Name("home")` |
| `kernel` | CLI Kernel, command registration, scheduling, command-to-command calls | `kernel.RegisterLazy("xxx", factory)` |
| `console` | Artisan-style command model, IO, table output | `console.NewDefinition("cmd:name")` |
| `config` | `.env` loading and dot-path configuration access | `config.GetString("app.name")` |
| `logger` | Multi-channel logging: stack/single/daily/stderr/null | `logger.Channel("daily").Info("msg")` |
| `database` | GORM connection management and connection pools | `database.Resolve()` |
| `database/schema` | Blueprint-style migration DSL | `schema.Bind(db).Create("table", fn)` |
| `cache` | Cache manager: memory/redis/file/failover | `cache.Remember(ctx, key, ttl, fn)` |
| `event` | Event bus: sync/async/queue | `event.Dispatch(ctx, ev)` |
| `exception` | Unified exception handler: Report + Render + log level mapping | `exception.Report(ctx, err, fields)` |
| `queue` | Job queue: Redis/RabbitMQ/Sync | `queue.Dispatch(ctx, job)` |
| `filesystem` | Filesystem abstraction: local/public/OSS | `filesystem.Disk("public").Put(...)` |
| `timer` | Scheduled task runner | `schedule.Command("x").Daily()` |
| `ratelimit` | Fixed-window rate limiting | `ratelimit.For("api").PerMinute(60)` |
| `cookie` | Cookie value object and queued writes | `cookie.New("name", "val").HttpOnly()` |
| `session` | Server-side sessions with the file driver | `session.Put(ctx, "key", value)` |
| `support` | General helpers: path resolution, value checks, type conversion, environment checks | `support.StoragePath(...)` / `support.IsProduction()` |

---

## Documentation

- [github.com/prismgo/docs](https://github.com/prismgo/docs)

---

## License

MIT
