package cookie

// QueueKey 是中间件在 gin.Context 中保存请求级 cookie 队列的键。
//
// 需求背景：session 中间件也需要安装同一条 cookie 队列，导出该键可以避免 session 包依赖 cookie
// 包的未导出实现细节。普通业务代码不需要直接使用该常量，应通过 QueueFrom 读取队列。
const QueueKey = "prismgo.cookie.queue"
