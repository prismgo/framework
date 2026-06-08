package console

import consolecontract "github.com/prismgo/framework/contracts/console"

// Command 描述新的统一命令格式。
//
// 用途：所有命令都通过 Definition 提供静态定义，通过 Handle 承担实际执行逻辑。
// 设计原因：彻底移除旧的 `Signature()/Description()/Handle()` 兼容接口后，命令注册、参数绑定、
// help 展示、测试与互调都能围绕同一套结构化模型收口，整体更简洁、易扩展、易维护。
type Command = consolecontract.Command
