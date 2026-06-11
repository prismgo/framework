<p align="center">
  <img src=".github/assets/logo.png" width="250">
</p>

<div align="center">

**PrismGo - Write Go Like Laravel**

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Module](https://img.shields.io/badge/module-github.com%2Fprismgo%2Fframework-blue)](https://github.com/prismgo/framework)
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE)

[简体中文](README.md) | English

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

Install the PrismGo installer:

```bash
go install github.com/prismgo/installer/cmd/prismgo@latest
```

Create an application:

```bash
prismgo new github.com/acme/myapp
```

Start the web server:

```bash
cd myapp
go run . serve
```

Open `http://localhost:8080/api` in your browser.

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
