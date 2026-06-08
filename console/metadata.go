package console

import consolecontract "github.com/prismgo/framework/contracts/console"

// CommandFactory 用于延迟创建命令实例。
//
// 用途：让业务装配层可以把命令注册表达为一组工厂函数，保持注册流程简洁、显式、易扩展。
type CommandFactory = consolecontract.CommandFactory
