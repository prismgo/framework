# 架构导航

## 定位

PrismGo 使用 Gin、GORM、Cobra、Viper、Logrus 和 go-redis 等成熟组件，提供 Laravel 风格的应用启动、Provider、Facade、路由、命令、缓存、事件和队列体验。

所有文件和符号的权威导航是 [`CODE_INDEX.md`](../CODE_INDEX.md)。修改 Go 代码前必须先读取它；本页只描述稳定的架构边界。

## 核心分层

| 层 | 位置 | 职责 |
|---|---|---|
| 公共契约 | `contracts/<component>/` | 定义组件接口，不放状态和具体构造逻辑 |
| 组件实现 | `<component>/` | 实现、Manager、配置、ServiceProvider 与组件 Facade |
| 通用解析 | `facade/` | 基于容器的泛型类型解析 |
| 应用编排 | `foundation/`、`provider/`、`container/` | 启动、注册、Boot、依赖解析与逆序释放 |
| 命令入口 | `kernel/`、`console/`、`cmd/` | 命令注册、参数解析与运行 |
| 内部复用 | `internal/` | 仅供框架内部共享的辅助实现 |

## 组件入口

| 领域 | 目录 |
|---|---|
| HTTP 与路由 | `http/`、`route/`、`responsekit/` |
| 数据与存储 | `database/`、`filesystem/`、`storage/`、`redis/` |
| 异步与运行时 | `queue/`、`event/`、`routine/`、`timer/`、`process/` |
| 应用服务 | `config/`、`cache/`、`session/`、`cookie/`、`logger/`、`translation/` |
| 基础能力 | `encoding/`、`encryption/`、`exception/`、`ratelimit/`、`support/` |

## 接入约定

- 组件通过 ServiceProvider 接入应用生命周期，沿用 `Name`、`Register`、`Boot` 的相邻实现模式。
- Facade 使用 `facade.Resolve[T](key)` 获取容器实例；解析失败只用于不可恢复的启动或配置错误。
- 容器使用 `Singleton`、`Instance`、`Bound`、`Make` 等既有能力；键名遵循 `"<component>.default"`。
- 配置统一经 `config` 包读取，使用点路径键，不绕过框架建立新的全局配置入口。
