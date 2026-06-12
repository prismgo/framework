<p align="center">
  <img src=".github/assets/logo.png" width="250">
</p>

<div align="center">

**PrismGo - Write Go Like Laravel**

[![Go Version](https://img.shields.io/badge/Go-1.25+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Module](https://img.shields.io/badge/module-github.com%2Fprismgo%2Fframework-blue)](https://github.com/prismgo/framework)
[![Coverage](https://codecov.io/gh/prismgo/framework/branch/main/graph/badge.svg)](https://codecov.io/gh/prismgo/framework)
[![Latest Version](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fproxy.golang.org%2Fgithub.com%2Fprismgo%2Fframework%2F%40latest&query=%24.Version&label=version)](https://pkg.go.dev/github.com/prismgo/framework?tab=versions)
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

---

## Documentation

- [github.com/prismgo/docs](https://github.com/prismgo/docs)

---

## Component Overview

| Component | What It Does | How to Use |
|---|---|---|
| `cache` | Cache manager: memory/redis/file/failover | `cache.Remember(ctx, key, ttl, fn)` |
| `config` | `.env` loading and dot-path configuration access | `config.GetString("app.name")` |
| `console` | Artisan-style command model, IO, table output | `console.NewDefinition("cmd:name")` |
| `container` | Service container: binding, resolution, singletons, instance management | `app.Container().Bind("key", factory)` |
| `cookie` | Cookie value object and queued writes | `cookie.New("name", "val").HttpOnly()` |
| `database` | GORM connection management and connection pools | `database.Resolve()` |
| `database/schema` | Blueprint-style migration DSL | `schema.Bind(db).Create("table", fn)` |
| `encryption` | Encryption and decryption: key configuration, string encrypter | `encryption.EncryptString("value")` |
| `event` | Event bus: sync/async/queue | `event.Dispatch(ctx, ev)` |
| `exception` | Unified exception handler: Report + Render + log level mapping | `exception.Report(ctx, err, fields)` |
| `filesystem` | Filesystem abstraction: local/public/OSS | `filesystem.Disk("public").Put(...)` |
| `foundation` | Application startup, Provider registration, lifecycle, resource closing | `foundation.NewApplication()` |
| `horizon` | Queue monitoring panel, worker management, job metrics, Dashboard | `go run . horizon` |
| `kernel` | CLI Kernel, command registration, scheduling, command-to-command calls | `kernel.RegisterLazy("xxx", factory)` |
| `logger` | Multi-channel logging: stack/single/daily/stderr/null | `logger.Channel("daily").Info("msg")` |
| `queue` | Job queue: Redis/RabbitMQ/Sync | `queue.Dispatch(ctx, job)` |
| `ratelimit` | Fixed-window rate limiting | `ratelimit.For("api").PerMinute(60)` |
| `route` | Laravel-style route declarations on top of Gin | `route.Get("/", handler).Name("home")` |
| `session` | Server-side sessions with the file driver | `session.Put(ctx, "key", value)` |
| `support` | General helpers: path resolution, value checks, type conversion, environment checks | `support.StoragePath(...)` / `support.IsProduction()` |
| `timer` | Scheduled task runner | `schedule.Command("x").Daily()` |

---

## License

MIT
