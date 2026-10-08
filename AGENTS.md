# AGENTS.md

任务循环（RunLoop）库，任务持久化到 MongoDB。纯 Go 库，无 `main` 包。

## 常用命令

- 构建/静态检查：`go build ./...`、`go vet ./...`
- 测试：`go test ./...`（当前仓库无任何测试文件）

## 架构与入口

- 模块 `github.com/ndsky1003/task`，Go 1.21.3。
- 唯一入口 `NewTaskMgr(serialize.ITaskSerialize, operator.IOperator, ...*options.Option)`（`mgr.go`）。参数为 nil 会 `panic`。
- 三个必须实现的接口：
  - `itask.ITask`（`itask/itask.go`，字段模板见文件顶部注释）
  - `operator.IOperator.HandleTask`（`operator/operator.go`）
  - `serialize.ITaskSerialize`（`serialize/i_serialize.go`）
- 内置序列化实现 `serialize.Mongo[T any](client, database, coll)`（`serialize/mongo.go`）。构造函数会创建索引，失败即 `panic`，需要真实可用的 MongoDB 连接。

## 关键陷阱

- `serialize.Mongo[T any]` 要求泛型 `T` 实现 `itask.ITask`，但只在 `Next`/`NextByType` 里做**运行时**类型断言，编译期不校验；断言失败返回 error。
- `ITask` 结构体需带 bson 标签的字段：`ID`、`Type`、`UpdateTime`、`CreateTime`、`Status`、`Order`。
- 并发槽 `ConcurrenceNum`（默认 10，上限 1000）必须**大于**有序任务类型数，否则有序任务会占满并发槽使普通任务饿死（见 `mgr.go` 顶部注释）。
- 有序任务（`IsOrder()==true`）同 `Type` 按 `CreateTime` 顺序串行执行；出错按 `OrderTaskHandleDelta` 递增重试。重试计数为 `uint8`，溢出到 255 时放弃该类型并视为 panic（`mgr.go` `sleep`）。
- 处理成功的任务会被直接删除（`Remove`），不存在「完成」状态残留。

## 约定

- 代码注释为中文，新增代码沿用中文注释风格。
- 日志通过 `SetLogger(ILogger)` 注入，默认 `default_logger`（`logger.go`）。
