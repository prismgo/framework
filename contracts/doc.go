// Package contracts 是 Prismgo 的公共能力边界根包。
//
// 需求背景：
// Prismgo 的 event、queue、cache 等框架能力需要在不互相导入具体实现包的情况下表达依赖。
// contracts 根包只作为文档入口，具体接口按能力域放在子包中。
//
// 设计思路：
// 子包只能声明 interface 和必要的公共数据类型（如 raw Payload），不能放置实现逻辑、
// 全局状态、facade slot、helper、默认值、构造函数或注册函数。实现包负责提供具体 struct、
// manager、facade 与 provider。
//
// 当前子包：
//   - event: Event, Listener, ListenerFunc, Dispatcher, Subscriber, ShouldQueue
//   - exception: ExceptionHandler
//   - queue: Job, Dispatcher, DispatchOptions, Queue, ReservedJob, Connector,
//     Factory, RestartStore, Middleware 及 22 个 Job 行为接口
//   - authz: Resolver
//   - cache: Store, Repository, Lock, Factory, TaggedRepository, MemoRepository,
//     FunnelLimiter 及驱动扩展接口（TouchStore, AtomicStore, BulkStore 等）
//   - console: Command, CommandFactory, Definition, Input, CommandCaller, Isolatable,
//     IO, Progress, CommandContext
//   - cookie: Signer
//   - encryption: Encrypter, StringEncrypter
//   - filesystem: Factory, Manager, Filesystem, Cloud, Repository
//   - logger: Driver, Logger
//   - session: Driver, Locker, Lock, Payload
//   - encoding: Codec
//   - routine: Task, Builder, Runner
//   - provider: ServiceProvider, NamedProvider, DeferrableProvider, TerminableProvider
//   - container: Resolver, Binder, Container
//   - translation: Loader, Translator, Selector, ParsedKey
package contracts
