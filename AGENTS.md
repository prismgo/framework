# PrismGo Framework

`github.com/prismgo/framework` 是 Go 1.25+ 的 Laravel 风格 Web 框架；本仓库包含框架实现、公共契约、测试与发布前验证。

## 铁律

| 规则 | 要求 |
|---|---|
| 先定位再修改 | 查找或修改 Go 代码前必须先读 [`CODE_INDEX.md`](CODE_INDEX.md)；新增包、文件或导出符号后同步更新索引。 |
| 控制变更边界 | 只改任务需要的内容；不擅自改变公共 API、增加第三方依赖、兼容层或回退逻辑。 |
| 守住生产语义 | 不为通过测试向生产代码加入测试专用分支、静默默认值或弱化校验；错误必须返回或报告。 |
| 测试与质量 | Go 行为变更需补测试，变更范围覆盖率目标不低于 90%；交付前执行相关测试、覆盖率、`gofmt` 和静态检查。 |
| 交付可追溯 | 功能变更按规范创建开发日志；如实报告检查结果、覆盖率、风险、死代码和兼容代码。 |

## 地图索引

| 何时读取 | 文档 | 用于确认 |
|---|---|---|
| 任务目标含糊、存在多种实现，或涉及扩大范围、公共 API、依赖、兼容逻辑与安全边界时 | [`docs/working-principles.md`](docs/working-principles.md) | 如何澄清需求、选择最小方案、控制变更范围并报告风险 |
| 开始定位代码，或需要判断功能应放在哪个包、契约、Provider、Facade、生命周期阶段时 | [`docs/architecture.md`](docs/architecture.md) | 稳定分层、组件入口、依赖方向与接入约定；具体文件和符号继续查 `CODE_INDEX.md` |
| 准备修改生产 Go 代码或测试，尤其涉及错误处理、公共行为、注释和测试装配时 | [`docs/go-development.md`](docs/go-development.md) | 编码约定、允许的错误处理方式、测试边界及禁止的测试修复手段 |
| 代码修改完成、准备验证或交付，或遇到测试失败、覆盖率不足、静态检查问题时 | [`docs/verification-and-delivery.md`](docs/verification-and-delivery.md) | 按变更范围选择命令、覆盖率要求、开发日志内容和最终报告清单 |
