<p align="center">
  <img src=".github/assets/logo.png" width="240">
</p>

<div align="center">

**PrismGo —— 像写 Laravel 一样写 Go**

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![Module](https://img.shields.io/badge/module-github.com%2Fprismgo%2Fframework-blue)](https://github.com/prismgo/framework)
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE)

简体中文 | [English](docs/README_en.md)

</div>

---

## 这是什么？

PrismGo 是一个由 AI Agent 全自动开发的 **Laravel 风格的 Go 语言 Web 框架**，Laravel 设计哲学贯穿始终，与 Go 社区主流编码风格自然融合。如果你熟悉 Laravel 的开发体验——Facade、ServiceProvider、Artisan 命令、缓存系统、Eloquent ORM 风格、事件系统、队列任务、日志系统——那么你会在 PrismGo 里找到一模一样的感觉。

我们希望让 Go 开发者不必在 "高性能" 和 "高开发效率" 之间做选择。PrismGo 使用 Go 生态中最成熟的底层组件（[Gin](https://github.com/gin-gonic/gin)、[GORM](https://github.com/go-gorm/gorm)、[Redis](https://github.com/redis/go-redis)、[Viper](https://github.com/spf13/viper)、[Logrus](https://github.com/sirupsen/logrus)、[Cobra](https://github.com/spf13/cobra)），再用 Laravel 的设计哲学把它们组织成一整套开箱即用的 Web 工具箱。

> **一句话定位：让你用 Go 的语法，享受 Laravel 的开发体验。**

---

## 为什么选择 PrismGo？

| | 裸用 Gin/GORM | PrismGo |
|---|---|---|
| **路由** | 手写 Gin Router | `route.Get("/users/{id}")` 命名路由、资源路由、分组 |
| **命令** | 裸写 main/flag | Artisan 风格 CLI：`go run . serve` `go run . migrate` |
| **配置** | 到处 viper.Get | `config.GetString("app.name")` 点路径统一读取 |
| **日志** | logrus 裸用 | 多通道日志：`logger.Channel("error").Error(...)` |
| **缓存** | 自己封装 Redis | `cache.Remember(ctx, key, ttl, callback)` |
| **事件** | 无 | `event.Dispatch(ctx, OrderPaid{ID: 1001})` + listener |
| **队列** | 自建 worker | `queue.Dispatch(ctx, job)` Redis/RabbitMQ 开箱即用 |
| **迁移** | 手写 SQL | Schema Blueprint：`$table->String("name")` |
| **资源管理** | 各自 Close | 统一应用生命周期，启动注册、退出释放 |

核心优势就一个：**你知道想做什么，框架帮你做掉样板代码。**

---

## 快速开始

### 安装

```bash
go get github.com/prismgo/framework
```

### 最小可运行示例

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

打开 `http://localhost:8000`，看到 `{"message": "Hello PrismGo!"}`。

---

## 组件全景图

| 组件 | 做什么 | 怎么用 |
|---|---|---|
| `foundation` | 应用启动、Provider 注册、生命周期、资源关闭 | `foundation.NewApplication()` |
| `horizon` | 队列监控面板，worker 管理、任务指标、Dashboard | `go run . horizon` |
| `route` | Gin 之上的 Laravel 风格路由声明 | `route.Get("/", handler).Name("home")` |
| `kernel` | CLI Kernel，命令注册、调度、互调 | `kernel.RegisterLazy("xxx", factory)` |
| `console` | Artisan 风格命令模型、IO、表格输出 | `console.NewDefinition("cmd:name")` |
| `config` | `.env` 加载、点路径配置读取 | `config.GetString("app.name")` |
| `logger` | 多通道日志：stack/single/daily/stderr/null | `logger.Channel("daily").Info("msg")` |
| `database` | GORM 连接管理、连接池 | `database.Resolve()` |
| `database/schema` | Blueprint 风格迁移 DSL | `schema.Bind(db).Create("table", fn)` |
| `cache` | 缓存管理器：memory/redis/file/failover | `cache.Remember(ctx, key, ttl, fn)` |
| `event` | 事件总线：同步/异步/队列 | `event.Dispatch(ctx, ev)` |
| `exception` | 统一异常处理器，Report + Render + 日志级别映射 | `exception.Report(ctx, err, fields)` |
| `queue` | 任务队列：Redis/RabbitMQ/Sync | `queue.Dispatch(ctx, job)` |
| `filesystem` | 文件系统抽象：local/public/OSS | `filesystem.Disk("public").Put(...)` |
| `timer` | 定时调度器 | `schedule.Command("x").Daily()` |
| `ratelimit` | 固定窗口限流 | `ratelimit.For("api").PerMinute(60)` |
| `cookie` | Cookie 值对象、队列写入 | `cookie.New("name", "val").HttpOnly()` |
| `session` | 服务端 session，file 驱动 | `session.Put(ctx, "key", value)` |
| `support` | 通用辅助函数：路径解析、值判断、类型转换、环境判断 | `support.StoragePath(...)` / `support.IsProduction()` |

---

## 文档

- [github.com/prismgo/docs](https://github.com/prismgo/docs)

---

## License

MIT
