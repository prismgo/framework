# PrismGo Framework — Code Index

> **Purpose:** AI Agent code-navigation index — rapidly locate modules, types, and function entry points.
> **Rule:** Before modifying any Go code, agents MUST read this file first to find the relevant symbols and files.
> **Scope:** All root packages + core sub-packages. `contracts/` lists interface summaries only; `internal/` lists sub-package names only.

---

<details>
<summary>📐 Architecture Overview</summary>

Three-layer pattern: `contracts/*` (interfaces) → `<component>/` (implementations) → `facade/` (generic resolver)

Foundation + Container → Kernel + Config → Component layer → Facade layer → CLI layer

</details>

---

## Quick Lookup Guide

> Quickly find a feature entry point? Start here.

| I want to... | Entry File | Key Symbols |
|---|---|---|
| **Bootstrap the application** | `foundation/application.go` → `foundation/application_builder.go` | `App`, `Builder`, `NewApplication()` |
| **Use DI container** | `container/container.go` → `container/helpers.go` | `Container`, `Make[T]()`, `Bind[T]()` |
| **Register routes** | `route/router.go` → `route/facade.go` | `Router`, `route.Get/Post/Group` |
| **Operate cache** | `cache/repository.go` → `cache/facade.go` | `Repository`, `cache.Put/Get/Remember` |
| **Dispatch queue jobs** | `queue/dispatcher.go` → `queue/facade.go` | `Dispatcher`, `queue.Dispatch/Bulk` |
| **Handle events** | `event/dispatcher.go` → `event/event.go` | `Dispatcher`, `Event`, `Listener` |
| **Manage sessions** | `session/store.go` → `session/manager.go` | `Store`, `Manager`, `session.Get/Put` |
| **File storage operations** | `filesystem/repository.go` → `filesystem/facade.go` | `Repository`, `filesystem.Put/Get/URL` |
| **Database ORM / migrations** | `database/` → `database/schema/` | `gorm.DB`, `Schema`, `Blueprint` |
| **Encryption / encoding** | `encryption/encrypter.go` → `encoding/encoding.go` | `Encrypter`, `Codec`, `JSON()/Msgpack()` |
| **Read configuration** | `config/config.go` → `config/facade.go` | `Config`, `config.Get/GetString` |
| **Register CLI commands** | `kernel/kernel.go` → `cmd/serve.go` | `Kernel`, `ServeCommand` |
| **Log messages** | `logger/manager.go` → `logger/facade.go` | `Manager`, `logger.Info/Debug/Error` |
| **Handle exceptions** | `exception/handler.go` → `exception/facade.go` | `Handler`, `exception.Report/Render` |
| **Internationalization** | `translation/translator.go` → `translation/facade.go` | `Translator`, `translation.Get/Choice` |

---

## foundation — Application Bootstrap & Lifecycle

**Path:** `foundation/`

| File | Key Symbols | Description |
|---|---|---|
| `application.go` | `App *Application` | Global Application singleton |
| | `Application` (struct) | Unified entry point for app bootstrap / provider lifecycle / cleanup |
| | `NewApplication(basePath ...string)` | Create an Application instance |
| `application_lifecycle.go` | `ErrApplicationShutdown` | Default shutdown reason error |
| | `(*Application) Context()` | Return the root lifecycle context |
| | `(*Application) Shutdown(reason...)` | Cancel the root lifecycle context |
| | `(*Application) RegisterShutdownSignals()` | Register SIGINT/SIGTERM signal handling |
| `application_providers.go` | `(*Application) RegisterProvider(provider)` | Append a ServiceProvider |
| | `(*Application) Boot()` | Execute the full provider lifecycle |
| `application_builder.go` | `Builder` (struct) | Laravel-style app configuration Builder |
| | `Configure(basePath...) *Builder` | Create a Builder |
| | `(*Builder) WithProviders(...)` | Declare project-level Providers |
| | `(*Builder) WithCommands(...)` | Declare console commands |
| | `(*Builder) WithRouting(configure func(*Routing))` | Declare routes / commands / schedule |
| | `(*Builder) WithMiddleware(configure func(*Middleware))` | Declare HTTP middleware |
| | `(*Builder) WithExceptions(configure func(*Exceptions))` | Declare exception handling |
| | `(*Builder) Create() *Application` | Build the Application |
| | `Routing` (struct) | Collects routes / commands / schedule / migration paths |
| | `Middleware` (struct) | Collects before-middleware / app-middleware |
| | `Exceptions` (struct) | Collects exception handler configuration |
| `application_cleanup.go` | `DefaultCloseTimeout` (15s) | Default resource release timeout |
| | `(*Application) RegisterCleanup(cleanup func)` | Register a cleanup function |
| | `(*Application) Close(timeout...) error` | Close the application |
| | `(*Application) CloseContext(ctx) error` | Context-aware close |
| `application_runner.go` | `Runner` (type `func(ctx) error`) | Blocking runner function signature |
| | `(*Application) RunContext(run Runner, contexts...)` | Start / run / close full flow |
| | `(*Application) Run(run Runner, contexts...)` | Same as RunContext, swallows close error |
| `runtime_registries.go` | `runtimeRegistries` (struct) | Runtime declarations holder |
| | `(*runtimeRegistries) NewHTTPServer(ctx, port)` | Create an HTTP Server |
| `container_helpers.go` | `(*Application) Make`, `Bind`, `Singleton`, `Instance`, `Alias`, `Resolved`, `Factory`, `Call` | Container delegation methods |

---

## container — Dependency Injection Container

**Path:** `container/`

| File | Key Symbols | Description |
|---|---|---|
| `container.go` | `Container` (struct) | DI Container implementation |
| | `NewContainer() *Container` | Create an empty container |
| | `(*Container) Bind(key, factory, opts...)` | Register a transient service |
| | `(*Container) Singleton(key, factory, opts...)` | Register a singleton service |
| | `(*Container) Instance(key, value, opts...)` | Register a pre-built instance |
| | `(*Container) Alias(key, alias)` | Create an alias |
| | `(*Container) Make(key)` | Resolve a service |
| | `(*Container) Call(callback, args...)` | Call a function with automatic dependency injection |
| | `(*Container) Close(ctx, groups...)` | Release resources by group |
| | `CloseGroup` (type), `CloseGroupNormal`, `CloseGroupReporting` | Shutdown phase enum |
| `helpers.go` | `SetProvider(func() *Container)` | Inject the current container provider |
| | `Make[T](key) (T, error)` | Package-level generic resolver |
| | `Bind[T]`, `Singleton[T]`, `Instance[T]` | Package-level generic binding |
| | `WithCloser[T]`, `WithContextCloser[T]`, `WithCloseGroup` | BindingOption factories |
| `doc.go` | — | Package documentation: describes the three-layer model |

---

## config — Configuration Management

**Path:** `config/`

| File | Key Symbols | Description |
|---|---|---|
| `config.go` | `Config` (struct) | Runtime configuration accessor |
| | `New() *Config` | Create an empty Config instance |
| | `(*Config) Reload()` | Reload configuration from `.env` |
| | `(*Config) Get(path, defaultValue...)` | Read a configuration value |
| | `(*Config) GetString`, `GetInt`, `GetBool`, `GetFloat64`, `GetStringMap` | Type-safe read methods |
| | `(*Config) Add(name, value)` | Set a configuration value |
| `facade.go` | `Resolve() *Config` | Resolve Config from container |
| | `Get`, `GetString`, `GetInt`, `GetBool`, `Clone`, `Reload`, `Empty` | Package-level facade |
| `runtime.go` | `Func` (type `func() map[string]any`) | Config file loader function |
| | `Add(name, fn Func)` | Register a config file |
| | `Boot() map[string]any` | Load all registered configs |
| `service_provider.go` | `ServiceProvider` | Register `config.default` lazy factory |

---

## kernel — CLI Kernel

**Path:** `kernel/`

| File | Key Symbols | Description |
|---|---|---|
| `kernel.go` | `Kernel` (struct) | Cobra-driven CLI Kernel |
| | `NewKernel(app, version)` | Create a Kernel |
| | `NewApplicationKernel(app, version, registry, deps...)` | Create a full application Kernel |
| | `(*Kernel) Register(commands...)` | Register commands |
| | `(*Kernel) Call(ctx, signature, input...)` | Execute a command |
| | `(*Kernel) CallSilently(ctx, signature, input...)` | Execute a command silently |
| | `(*Kernel) Run(ctx, argv)` | Run the CLI main flow |
| | `(*Kernel) Starting(callback...)` | Register startup callbacks |
| `call.go` | `CallInput` (type `map[string]string`) | Input for programmatic command calls |
| `starting.go` | `StartingCallback` (type) | Startup callback function signature |
| `builtins.go` | `BuiltinDependencies` (struct) | Builtin command dependencies |
| | `RegisterBuiltinCommands(kernel, deps)` | Register all builtin commands |
| `help.go` | — | Cobra help template customization |
| `completion.go` | — | Shell completion command registration |
| `missing_input.go` | — | Interactive prompt for missing arguments |
| `application_facade.go` | `ArtisanFacadeKey` (const = `"artisan.kernel"`) | Container key for console Kernel |
| | `BindApplicationKernel(app)` | Bind console Kernel to Application container |
| `application_kernel.go` | `NewApplicationKernel(app, version, registry, deps...)` | Create Kernel with app commands & schedule pre-wired |
| `application_registry.go` | `ApplicationRegistrySource` (interface) | Read-only registry exposing commands/schedules/migrations |
| | `StartingCallback`, `StartingRegistrar`, `WithApplicationRegistry` | Startup callback types and option |
| `call_input.go` | (types for command argument parsing) | Command input argument / option handling |

---

## cmd — Built-in CLI Commands

**Path:** `cmd/`

| File | Key Symbols | Description |
|---|---|---|
| `serve.go` | `ServeCommand` | Start the HTTP server |
| `make_command.go` | `MakeCommand` | Code generator: `go run . make <type>` |
| `migrate_command.go` | `MigrateCommand` | Run database migrations |
| `queue_command.go` | `QueueCommand` | Start queue worker |
| `cron_command.go` | `CronCommand` | Start cron scheduler |
| `storage_link.go` | `StorageLinkCommand` | Create storage symlinks |
| `vendor_publish.go` | `VendorPublishCommand` | Publish vendor assets |

**Sub-packages:**
| Sub-package | Purpose |
|---|---|
| `cmd/make/` | Code generation templates and logic |
| `cmd/queue/` | Queue-specific command wiring |
| `cmd/migration/` | Migration command definitions (`migrate`, `rollback`, `status`) |
| `cmd/cron/` | Cron command wiring |
| `cmd/make/builder/` | Builder logic for code generation |

---

## console — Console Command Base & IO

**Path:** `console/`

| File | Key Symbols | Description |
|---|---|---|
| `command.go` | `Command` (struct) | Base Artisan-style command |
| | `(*Command) Call/Silent/Info/Error/Line/Task/Table` | IO output methods |
| | `(*Command) Argument/Arguments/Option/Options/Confirm/Ask/Choice` | IO input methods |
| `formatter.go` | — | Output formatting styles |
| `helpers.go` | `ParseSignature(sig) *Signature` | Parse command signatures |
| | `Signature` (struct), `InputArgument`, `InputOption` | Signature components |
| `output.go` | `Output` (interface) | Output writer contract |
| | `Line`, `Info`, `Warn`, `Error`, `Table`, `Task` | Structured output helpers |
| `service_provider.go` | `ServiceProvider` | Register `console.command` singleton |

---

## route — Route Registration

**Path:** `route/`

| File | Key Symbols | Description |
|---|---|---|
| `router.go` | `Router` (struct) | Gin-based route manager |
| | `NewRouter(container, event)` | Create a Router |
| | `(*Router) Get/Post/Put/Delete/Any/Match/Patch/Options/Static` | HTTP method routing |
| | `(*Router) Group/Resource/Prefix/Name/Domain/Middleware/Controller` | Route group/organization |
| | `(*Router) Mount(engine)`, `List()`, `URL(name, params)` | Route mounting and URL generation |
| `registrar.go` | `Registrar` (struct) | Route builder for fluent registration |
| `resource.go` | `Resource` (struct), `ResourceRegistrar` | RESTful resource route registration |
| | `Route`, `HandlerFunc` (type) | Common route types re-exported from Gin |
| `facade.go` | `Resolve() *Router` | Package-level facade |
| | `Get`, `Post`, `Put`, `Delete`, `Patch`, `Any`, `Options`, `Match`, `Redirect`, `Static`, `Fallback`, `Group`, `Resource`, `Prefix`, `Name`, `Domain`, `Middleware`, `Controller`, `Mount`, `List`, `URL` | Full routing facade |
| `service_provider.go` | `ServiceProvider` | Register `route.router` lazy singleton |

---

## http — HTTP Server

**Path:** `http/`

| File | Key Symbols | Description |
|---|---|---|
| `server.go` | `Server` (struct) | Gin HTTP server lifecycle |
| | `NewServer(app, port)` | Create an HTTP Server |
| | `(*Server) Start(ctx)` | Start the server |
| | `(*Server) Shutdown(ctx)` | Graceful server shutdown |
| `middleware.go` | `Middleware` (struct) | Middleware stack definition |
| | `Use(middleware)`, `Push(name, middleware)`, `Register(name, middleware)` | Middleware registration |
| `request_id.go` | — | Request ID middleware |
| `process.go` | — | Process management middleware |
| `gin.go` | `Gin() *gin.Engine` | Access the underlying Gin engine |
| `guard.go` | — | Route guard utilities |
| `service_provider.go` | `ServiceProvider` | Register `http.server` lazy singleton |

**Sub-package:**
| Sub-package | Purpose |
|---|---|
| `http/middleware/` | Built-in HTTP middleware collection |

---

## cache — Multi-Driver Cache

**Path:** `cache/`

| File | Key Symbols | Description |
|---|---|---|
| `manager.go` | `Manager` (struct) | Cache driver manager |
| `repository.go` | `Repository` (struct) | Core cache operations |
| | `(*Repository) Get`, `Put`, `Forever`, `Forget`, `Flush`, `Has`, `Add`, `Pull`, `Sear`, `Remember`, `RememberForever`, `Increment`, `Decrement`, `Tags`, `Funnel`, `Lock`, `WithoutOverlapping` | Cache operations |
| `lock.go` | `DistributedLock` (struct) | Distributed lock via cache |
| `config.go` | `Config`, `StoreConfig` | Cache configuration types |
| `driver.go` | `DriverFactory`, `Extend(name, factory)` | Driver registration |
| `facade.go` | `Resolve() cachecontract.Factory` | Package-level facade |
| | `Default`, `Store`, `Put`, `Get`, `Remember`, `Flexible`, `Forget`, `Flush`, `Add`, `Forever`, `Pull`, `Has`, `Sear`, `Tags`, `Funnel`, `Lock`, `WithoutOverlapping` | Full cache facade API |
| `service_provider.go` | `ServiceProvider` | Register `cache.manager` lazy singleton |

---

## queue — Job Queue

**Path:** `queue/`

| File | Key Symbols | Description |
|---|---|---|
| `manager.go` | `Manager` (struct) | Queue connection manager |
| `dispatcher.go` | `Dispatcher` (struct) | Job dispatcher |
| | `(*Dispatcher) Dispatch(job, options...)` | Dispatch a job |
| | `(*Dispatcher) Later(seconds, job, options...)` | Delayed dispatch |
| | `(*Dispatcher) Batch(jobs...) *BatchBuilder` | Batch dispatch |
| `worker.go` | `Worker` (struct) | Queue worker |
| `registry.go` | `Registry` (struct) | Job type registry |
| | `Register/RegisterJob/job` | Job registration |
| `config.go` | `Config`, `ConnectionConfig` | Queue configuration types |
| `middleware.go` | `Middleware` (type) | Queue middleware |
| `redis_queue.go` | `redisQueue` | Redis-backed queue connector |
| `sync_queue.go` | `syncQueue` | Synchronous queue connector |
| `rabbitmq_queue.go` | `rabbitmqQueue` | RabbitMQ-backed queue connector |
| `facade.go` | `Resolve() *Manager` | Package-level facade |
| | `Dispatch`, `Batch`, `Later`, `Extend`, `UseMiddleware`, `Failed`, `RequestRestart`, `Close`, `GetBatchStatus`, `CancelBatch`, `MarkBatchJob`, `DelaySeconds` | Full queue facade API |
| `service_provider.go` | `ServiceProvider` | Register `queue.manager` lazy singleton |

---

## event — Event System

**Path:** `event/`

| File | Key Symbols | Description |
|---|---|---|
| `dispatcher.go` | `Dispatcher` (struct) | Event dispatcher |
| | `NewDispatcher(container)` | Create a Dispatcher |
| | `(*Dispatcher) Dispatch(ctx, event)` | Fire an event |
| | `(*Dispatcher) Listen(event, listener)` | Register a listener |
| | `(*Dispatcher) Subscribe(subscriber)` | Register a subscriber |
| | `(*Dispatcher) HasListeners(event)`, `Flush()`, `Forget()` | Listener management |
| `event.go` | `Event` (struct) | Base event |
| | `Listener` (interface) | Single listener contract |
| | `ListenerFunc` (type) | Function-based listener |
| | `ShouldQueue` (interface) | Queued listener marker |
| | `ShouldBroadcast` (interface) | Broadcasting listener marker |
| `facade.go` | `Resolve() eventcontract.Dispatcher` | Package-level facade |
| | `Dispatch`, `Listen`, `ListenFunc`, `Subscribe`, `Forget`, `Has` | Full event facade API |
| `service_provider.go` | `ServiceProvider` | Register `event.dispatcher` lazy singleton |

---

## redis — Redis Manager

**Path:** `redis/`

| File | Key Symbols | Description |
|---|---|---|
| `manager.go` | `Manager` (struct) | Redis connection pool manager |
| | `NewManager(cfg)` | Create a Redis Manager |
| | `(*Manager) Connection(name...)` | Get a named Redis connection |

---

## session — Session Management

**Path:** `session/`

| File | Key Symbols | Description |
|---|---|---|
| `manager.go` | `Manager` (struct) | Session manager |
| | `(*Manager) Start(ctx, r, w)` | Start a session |
| | `(*Manager) Close()` | Close session manager |
| `store.go` | `Store` (struct) | Session lifecycle operations |
| `config.go` | `Config`, `RedisConfig`, `CookieConfig`, `LockConfig` | Session configuration types |
| `driver.go` | `DriverFactory`, `Extend(name, factory)`, `ResolveDriver(name, cfg)` | Driver registration and resolution |
| `redis_driver.go` | (implements `Driver`) | Redis-backed session driver |
| `file_driver.go` | (implements `Driver`) | File-backed session driver |
| `facade.go` | `Resolve() *Manager` | Package-level facade |
| | `Start`, `Get`, `Put`, `Has`, `Exists`, `Missing`, `Pull`, `Forget`, `Flush`, `Flash`, `Now`, `Reflash`, `Keep`, `Regenerate`, `Invalidate`, `Save` | Full session facade API |
| `flash.go` | `(*Store) Flash`, `Now`, `Reflash`, `Keep` | Flash data operations |
| `lock.go` | — | File lock and session lock |
| `middleware.go` | `MiddlewareOption`, `StartSession` middleware factory |
| `service_provider.go` | `ServiceProvider` | Register `session.manager` lazy singleton |

---

## encryption — Encryption

**Path:** `encryption/`

| File | Key Symbols | Description |
|---|---|---|
| `encrypter.go` | `Encrypter` (struct) | AES-based encryption |
| | `NewEncrypter(key)` | Create an Encrypter |
| | `(*Encrypter) Encrypt(plaintext)` | Encrypt a value |
| | `(*Encrypter) Decrypt(ciphertext)` | Decrypt a value |
| | `(*Encrypter) EncryptString(plaintext)` | Encrypt a string |
| | `(*Encrypter) DecryptString(ciphertext)` | Decrypt a string |
| `facade.go` | `Resolve() *Encrypter` | Package-level facade |
| | `Encrypt`, `Decrypt`, `EncryptString`, `DecryptString` | Full encryption facade API |

---

## encoding — Codec

**Path:** `encoding/`

| File | Key Symbols | Description |
|---|---|---|
| `encoding.go` | `Codec` (interface) | Encode/decode contract |
| | `JSON()` | JSON codec instance |
| | `Msgpack()` | MessagePack codec instance |
| | `NewJSONCodec()`, `NewMsgpackCodec()` | Codec constructors |

---

## logger — Logging

**Path:** `logger/`

| File | Key Symbols | Description |
|---|---|---|
| `manager.go` | `Manager` (struct) | Log channel manager |
| | `NewManager(cfg)` | Create a Manager |
| `config.go` | `Config`, `ChannelConfig` | Logger configuration types |
| `driver.go` | `DriverFactory`, `Extend(name, factory)` | Driver registration |
| `facade.go` | `Resolve() *Manager` | Package-level facade |
| | `Info`, `Debug`, `Warn`, `Error`, `Fatal`, `Panic`, `WithFields`, `Channel` | Full logger facade API |
| `service_provider.go` | `ServiceProvider` | Register `logger.manager` lazy singleton |

---

## filesystem — File Storage

**Path:** `filesystem/`

| File | Key Symbols | Description |
|---|---|---|
| `manager.go` | `Manager` (struct) | Multi-disk filesystem manager |
| `repository.go` | `Repository` (struct) | Core filesystem operations |
| | `(*Repository) Put`, `Get`, `Delete`, `Exists`, `Copy`, `Move`, `URL`, `TemporaryURL`, `Size`, `LastModified`, `Files`, `Directories` | Filesystem operations |
| `local.go` | `localDriver` | Local filesystem driver |
| `oss.go` | `ossDriver` | Alibaba Cloud OSS driver |
| `driver.go` | `Driver`, `DriverFactory`, `Extend(name, factory)` | Driver registration |
| `facade.go` | `Resolve() *Manager` | Package-level facade |
| | `Default`, `Disk`, `Put`, `Get`, `Exists`, `Delete`, `Copy`, `Move`, `URL`, `TemporaryURL`, `Size`, `LastModified`, `Files`, `Directories`, `PutFile`, `OpenStream`, `Download`, `MakeDirectory`, `DeleteDirectory` | Full filesystem facade API |
| `service_provider.go` | `ServiceProvider` | Register `filesystem.manager` lazy singleton |
| `config.go` | `Config`, `LocalConfig`, `OSSConfig`, `S3Config` | Filesystem configuration types |

---

## database — Database ORM & Schema

**Path:** `database/`

| File | Key Symbols | Description |
|---|---|---|
| `manager.go` | `Manager` (struct) | Database connection manager |
| `config.go` | `Config`, `ConnectionConfig` | Database configuration types |
| `facade.go` | `Resolve() *Manager` | Package-level facade |
| | `Connection`, `DB`, `Table` | Database facade |
| `service_provider.go` | `ServiceProvider` | Register `database.manager` lazy singleton |
| `factory.go` | — | Model factory support |
| `seeder.go` | — | Database seeding |

**Sub-packages:**
| Sub-package | File | Key Symbols | Description |
|---|---|---|---|
| `database/schema/` | `schema.go` | `Schema` (struct) | Schema builder |
| | `blueprint.go` | `Blueprint` (struct) | Table blueprint definition |
| | `facade.go` | `Resolve() *Builder` | Schema facade: `Create`, `Table`, `DropIfExists`, `HasColumn`, `HasColumnType`, `GetColumnListing`, `GetIndexes`, `HasIndex`, `HasTable`, `HasView`, `GetTables`, `GetViews`, `GetTypes`, `GetForeignKeys`, `CreateDatabase`, `DropDatabaseIfExists` |
| | `grammar.go` | — | SQL grammar definitions |
| `database/migrations/` | — | Migration execution engine |

---

## exception — Exception Handler

**Path:** `exception/`

| File | Key Symbols | Description |
|---|---|---|
| `handler.go` | `Handler` (struct) | Exception handler |
| | `NewHandler()` | Create a Handler |
| | `(*Handler) Report(ctx, err)` | Report an exception |
| | `(*Handler) Render(ctx, err)` | Render an exception to HTTP response |
| | `(*Handler) ShouldReport(err)`, `ShouldRender(err)` | Decision helpers |
| `facade.go` | `Resolve() *Handler` | Package-level facade |
| | `Report`, `Render`, `ShouldReport`, `ShouldRender` | Full exception facade API |
| `service_provider.go` | `ServiceProvider` | Register `exception.handler` lazy singleton |

---

## provider — ServiceProvider Contract & Vendor Publish

**Path:** `provider/`

| File | Key Symbols | Description |
|---|---|---|
| `service_provider.go` | `ServiceProvider` (interface) | `Register(app)`, `Boot(app)` |
| | `PublishingServiceProvider` | Provider with publishable assets |
| | `IsDeferredServiceProvider` (interface) | Deferred/lazy provider marker |
| `vendor_publish.go` | — | Vendor asset publishing |

---

## timer — Cron / Scheduled Tasks

**Path:** `timer/`

| File | Key Symbols | Description |
|---|---|---|
| `schedule.go` | `Schedule` (struct) | Task scheduler |
| | `NewSchedule()` | Create a Schedule |
| | `(*Schedule) Command(signature) *ScheduledTask` | Schedule a CLI command |
| | `(*Schedule) Call(fn) *ScheduledTask` | Schedule a callback |
| | `(*Schedule) Start(ctx)` | Start the scheduler loop |
| `scheduled_task.go` | `ScheduledTask` (struct) | A single scheduled task definition |
| | `Daily`, `Hourly`, `EveryMinute`, `EveryFiveMinutes`, `EveryTenMinutes`, `Cron(expr)`, `Days`, `WithoutOverlapping`, `OnOneServer`, `RunInBackground`, `AppendOutputTo`, `Then` | Task scheduling frequency and options |
| `config.go` | — | Timer configuration |

---

## cookie — Cookie Management

**Path:** `cookie/`

| File | Key Symbols | Description |
|---|---|---|
| `cookie.go` | `Cookie` (struct) | Signed cookie management |
| | `NewCookie(encrypter)` | Create a Cookie manager |
| | `(*Cookie) Set(ctx, name, value, minutes)` | Set a signed cookie |
| | `(*Cookie) Get(ctx, name)` | Get a signed cookie |
| | `(*Cookie) Forget(ctx, name)` | Remove a cookie |
| | `(*Cookie) Queue(name, value, minutes)` | Queue a cookie for next response |

---

## artisan — Artisan Kernel

**Path:** `artisan/`

| File | Key Symbols | Description |
|---|---|---|
| `kernel.go` | `Artisan` (struct) | Console application kernel |
| `command.go` | — | Artisan command definitions |

---

## contracts — Public Component Interfaces

**Path:** `contracts/`

| Sub-package | Key Interfaces | Description |
|---|---|---|
| `contracts/cache` | `Factory`, `Repository` | Cache contracts |
| `contracts/queue` | `Factory`, `Job`, `ShouldQueue` | Queue contracts |
| `contracts/event` | `Dispatcher` | Event dispatcher contract |
| `contracts/filesystem` | `Factory`, `Repository` | Filesystem contracts |
| `contracts/logger` | `Logger` | Logger contract |
| `contracts/encryption` | `Encrypter` | Encryption contract |
| `contracts/encoding` | `Codec` | Codec contract |

---

## horizon — Queue Dashboard

**Path:** `horizon/`

| File | Key Symbols | Description |
|---|---|---|
| `horizon.go` | `Horizon` (struct) | Queue monitoring dashboard |
| `service_provider.go` | `ServiceProvider` | Register `horizon.dashboard` lazy singleton |

---

## support — Helpers & Utilities

**Path:** `support/`

| File | Key Symbols | Description |
|---|---|---|
| `env.go` | `Env(key, defaultValue...)` | Environment variable reader |
| | `EnvFilePath`, `EnvExists`, `EnvOr`, `EnvMap` | Environment helpers |
| `collection.go` | `Collection` (struct) | Fluent array/collection helper |

---

## process — Process Supervision & Signals

**Path:** `process/` — (currently no exported symbols at package level)

---

## ratelimit — Rate Limiter

**Path:** `ratelimit/`

| File | Key Symbols | Description |
|---|---|---|
| `ratelimit.go` | `RateLimiter` (struct) | Rate limiter engine |
| | `RateLimiterFunc`, `LimiterFunc` | Limiter function types |
| | `For(name, limiter)`, `Limiter(name)`, `ShouldHashKeys(shouldHash)` | Limiter registration |
| `config.go` | — | Rate limit configuration |
| `facade.go` | `Resolve() *RateLimiter` | Package-level facade |
| | `For`, `Limiter`, `Attempt`, `TooManyAttempts`, `Hit`, `Increment`, `Decrement`, `Attempts`, `ResetAttempts`, `Remaining`, `RetriesLeft`, `Clear`, `AvailableIn` | Full ratelimit facade API |

---

## routine — Goroutine Task Builder

**Path:** `routine/`

| File | Key Symbols | Description |
|---|---|---|
| `routine.go` | `Routine` (struct) | Goroutine task builder/runner |

---

## translation — Internationalization (i18n)

**Path:** `translation/`

| File | Key Symbols | Description |
|---|---|---|
| `translator.go` | `Translator` (struct) | i18n translator |
| | `(*Translator) Get(key, replace, locale)` | Translate a key |
| | `(*Translator) Choice(key, number, replace, locale)` | Pluralized translation |
| | `(*Translator) Has(key, locale)`, `Locale()`, `SetLocale(locale)` | Locale management |
| `loader.go` | `Loader` (struct) | Translation file loader |
| | `Load(locale, group, namespace)` | Load translations for a locale |
| `message_selector.go` | `MessageSelector` (struct) | Pluralization rule engine |
| `facade.go` | `Resolve() transcontract.Translator` | Package-level facade |
| | `Get`, `Choice`, `Has`, `HasForLocale`, `Locale`, `CurrentLocale`, `SetLocale`, `IsLocale`, `GetFallback`, `SetFallback`, `AddNamespace`, `AddPath`, `AddJSONPath`, `AddLines`, `Stringable`, `HandleMissingKeysUsing`, `DetermineLocalesUsing`, `GetMap`, `Loader`, `Reset` | Full translation facade API |
| `service_provider.go` | `ServiceProvider` | Register `translation.translator` lazy singleton |

---

## internal — Shared Helpers

**Path:** `internal/`

| Sub-package | Purpose |
|---|---|
| `internal/fmtx/` | Extended formatting utilities |
| `internal/optional/` | Optional value type |
| `internal/path/` | Path resolution helpers |
| `internal/runtimex/` | Runtime introspection (GoroutineID) |
| `internal/stackx/` | Stack trace utilities |

---

## facade — Generic Typed Resolver

**Path:** `facade/`

| File | Key Symbols | Description |
|---|---|---|
| `facade.go` | `Resolve[T any](key string) T` | Generic service resolver from Application container. Panics on misconfiguration (container not set, key not bound, type mismatch, or factory error). |

**Layer role:** The third layer of the three-layer pattern. Each component facade uses `facade.Resolve[T](key)` internally to resolve its managed instance from the container.

---

## version — Framework Version Info

**Path:** `version/`

| File | Key Symbols | Description |
|---|---|---|
| `version.go` | `Name` (const = `"PrismGo"`) | Framework name |
| | `Framework` (type) | Version info struct |
| | `Banner` | CLI startup banner |

---

## responsekit — HTTP Response Builders

**Path:** `responsekit/`

| File | Key Symbols | Description |
|---|---|---|
| `deferred_response.go` | `DeferredResponseCommitter` (struct) | Deferred HTTP response writer |
| | `DeferredResponseWriter` (struct) | Buffered response + commit |

---

## Container Key Naming Convention

| Container Key | Component | Registration |
|---|---|---|
| `container.default` | container | `foundation` |
| `config.default` | config | `config.ServiceProvider` |
| `kernel.default` | kernel | `foundation` |
| `artisan.kernel` | kernel | `BindApplicationKernel()` |
| `kernel.starting.registrar` | kernel | `foundation` |
| `route.router` | route | `route.ServiceProvider` |
| `http.server` | http | `http.ServiceProvider` |
| `cache.manager` | cache | `cache.ServiceProvider` |
| `queue.manager` | queue | `queue.ServiceProvider` |
| `event.dispatcher` | event | `event.ServiceProvider` |
| `session.manager` | session | `session.ServiceProvider` |
| `exception.handler` | exception | `exception.ServiceProvider` |
| `logger.manager` | logger | `logger.ServiceProvider` |
| `filesystem.manager` | filesystem | `filesystem.ServiceProvider` |
| `database.manager` | database | `database.ServiceProvider` |
| `translation.translator` | translation | `translation.ServiceProvider` |
| `console.command` | console | `console.ServiceProvider` |
| `horizon.dashboard` | horizon | `horizon.ServiceProvider` |

---

## Key Invocation Flows

| Scenario | Invocation Chain |
|---|---|
| **HTTP request lifecycle** | `route.Router.Mount(gin)` → `http.Server.Start()` → `session.Middleware` → `route.Handler` → `exception.Handler.Render()` |
| **Cache + Queue cooperation** | `queue.Dispatcher.Dispatch(job)` → `cache.Repository.Remember(key, ttl, fn)` → `redis`/`memory` driver |
| **Event → Queue bridge** | `event.Dispatcher.Dispatch(event)` → `ShouldQueue` listener → `queue.Dispatcher.Dispatch(job)` |
| **CLI command execution** | `kernel.Kernel.Run(argv)` → `console.Command.Handle()` → `config.Get()` / `logger.Info()` |
| **Application startup** | `Builder.Create()` → `Application.Boot()` → providers → `server.Start()` / `schedule.Start()` |

---

## File Naming Conventions

| File Pattern | Purpose |
|---|---|
| `service_provider.go` | DI container registration, service registration & booting |
| `facade.go` | Public convenience API |
| `manager.go` / `config.go` / `types.go` | Core logic / configuration / types |
| `driver.go` / `*_driver.go` | Driver registration and implementation (DriverFactory, Extend) |

---

## Memory Hooks

> Keyword anchors to activate Claude Memory.

| Keywords | Related File |
|---|---|
| application bootstrap, lifecycle, app builder, provider registration | `foundation/application*.go` |
| DI, bind, singleton, make, resolve | `container/container.go` |
| config get, dot path, env, viper | `config/config.go` |
| route, router, gin handler, group, middleware, resource | `route/router.go` |
| cache remember, lock, ttl, store | `cache/repository.go` |
| queue job, dispatch, batch, worker | `queue/dispatcher.go` |
| event, listener, subscribe, should-queue | `event/dispatcher.go` |
| session start, store, flash | `session/store.go` |
| file put, get, disk, oss, s3 | `filesystem/repository.go` |
| schema migration, blueprint, table | `database/schema/schema.go` |
| logger, channel, log level | `logger/manager.go` |
| exception report, render | `exception/handler.go` |
| schedule cron, daily, hourly | `timer/schedule.go` |
| ratelimit attempt, throttle | `ratelimit/ratelimit.go` |
| translation i18n, choice, plural | `translation/translator.go` |
| kernel, artisan, cli command | `kernel/kernel.go` |
| facade.Resolve, generic resolver | `facade/facade.go` |
