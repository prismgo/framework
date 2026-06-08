# PrismGo

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

## 核心特性一览

## 🎭 Facade：像调全局函数一样调用服务

```go
cache.Put(ctx, "key", value, ttl)           // 缓存
logger.Channel("error").Error("failed")     // 日志
event.Dispatch(ctx, ev)                      // 事件
queue.Dispatch(ctx, job)                     // 队列
db := database.Resolve()                    // 数据库
```

和 Laravel Facade 一样方便，底层通过 Application 容器管理生命周期，没有全局变量污染和初始化顺序问题。

---

## 🧩 Provider 驱动架构

PrismGo 采用和 Laravel 一模一样的 ServiceProvider 模式来组织功能：

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

每个功能模块通过 Provider 注册、懒加载、释放资源。你只需要在 `bootstrap/app.go` 中声明：

```go
foundation.Configure().
    WithProviders(
        &cache.ServiceProvider{},
        &queue.ServiceProvider{},
        &filesystem.ServiceProvider{},
    )
```

框架会自动按依赖顺序加载，并在退出时逆序释放所有资源。

---

### 🎨 Laravel 风格路由

```go
route.Prefix("/api").Middleware(auth).Group(func(r route.Registrar) {
    r.Get("/users", userController.Index).Name("users.index")
    r.Post("/users", userController.Store).Name("users.store")
    r.Get("/users/{id}", userController.Show).Name("users.show")
})
```

命名路由、分组嵌套、中间件、参数约束 —— 和 Laravel Router 一样的写法。

---

### 🖥 Artisan 风格命令行

```bash
go run . serve --port=8051          # 启动 HTTP 服务
go run . migrate                    # 数据库迁移
go run . db:seed                    # 数据填充
go run . queue --queue=default      # 启动队列消费者
go run . cron                       # 启动定时任务
```

注册自定义命令和 Laravel 一样简单。定义命令只需实现 `Definition()` 和 `Handle()` 两个方法：

```go
// app/cmd/report_daily.go
type DailyReportCommand struct{}

func (c *DailyReportCommand) Definition() *console.Definition {
    return console.MustDefinition(
        "report:daily {date? : 报表日期，默认今天} {--e|email= : 发送到指定邮箱}",
        "生成每日营业报表",
    )
}

func (c *DailyReportCommand) Handle(ctx console.CommandContext) error {
    date := ctx.Input().Argument("date")
    email := ctx.Input().Option("email")
    // 业务逻辑...
    ctx.IO().Success("日报已生成：%s", date)
    return nil
}
```

注册：

```go
// bootstrap/app.go
foundation.Configure().
    WithRouting(func(r *foundation.Routing) {
        r.Commands(
            func() console.Command { return NewDailyReportCommand() },
        )
    })
```

`MustDefinition` 接受一段 Artisan 风格的签名 DSL，自动解析参数类型、必填/可选、短选项别名，零手写 Cobra 绑定代码。

---

### ⏱ 定时任务

```go
schedule.Command("report:daily --force").DailyAt("08:00")
schedule.Command("cache:clean").EveryThirtyMinutes()
```

在 `cron` 命令中启动，一行配置即可。

---

### 📦 事件系统

```go
event.ListenFunc("order.paid", func(ctx context.Context, e event.Event) error {
    order := e.(*OrderPaid)
    notification.Send(ctx, order.UserID, "您的订单已支付")
    return nil
})

event.Dispatch(ctx, &OrderPaid{ID: order.ID})
```

同步、异步 goroutine、队列监听器三种模式随心切换。

---

### ⏳ 队列任务

只需要实现一个接口：

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

支持重试、超时、唯一任务、批次、链式任务，驱动支持 Redis / RabbitMQ / Sync。

---

### 🗃 缓存系统

```go
cache.Put(ctx, "user:1", user, 10*time.Minute)

user, err := cache.Get[User](ctx, "user:1")

user, err := cache.Remember(ctx, "stats:daily", 1*time.Hour, func() (*Stats, error) {
    return computeStats(ctx)
})
```

驱动支持 memory / redis / file / failover，还有 `Tags`、`Flexible`（stale-while-revalidate）、`Lock`（分布式锁）、`Memo`（请求级记忆化）等高级功能。

---

### 📁 文件系统

```go
disk := filesystem.Disk("public")

disk.PutFileAs(ctx, "avatars", uploadedFile, "user_123.jpg")

url := disk.URL("avatars/user_123.jpg")
```

统一 Disk 抽象，支持 local / public / 阿里云 OSS，切换驱动只需改配置。

---

### 📝 多通道日志

```go
logger.Channel("daily").WithFields(logrus.Fields{
    "user_id": userID,
}).Info("用户登录成功")

logger.Channel("error").WithError(err).Error("订单同步失败")
```

支持 stack / single / daily / stderr / null 多种驱动，通道按需懒加载，构造失败时自动回退默认通道，不会因为日志系统故障影响业务可用性。

---

### 🗄 数据库 & Schema 迁移

用 Blueprint 写迁移，而不是手写 SQL：

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

GORM 作为底层 ORM，ElasticSearch 风格的查询：

```go
db.Where("status = ?", "paid").Order("created_at desc").Find(&orders)
```

---

### 📊 速率限制

```go
ratelimit.For("api").Limit(60).PerMinute()
```

中间件直接挂载，共享缓存存储，支持自定义超限响应。

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
