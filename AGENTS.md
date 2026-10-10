# AGENTS.md

任务循环（RunLoop）库，任务持久化到 MongoDB 或 SQLite。纯 Go 库，无 `main` 包。

## 常用命令

- 构建/静态检查：`go build ./...`、`go vet ./...`
- 测试：`go test -race ./...`（MongoDB 集成测试需要真实可用的 MongoDB，默认 `mongodb://localhost:27017`，可用 `MONGO_URI` 覆盖；连不上会跳过。SQLite 测试用内存库，无需外部依赖）
- 性能对比：`go test -bench=. -benchmem -run=^$ ./serialize/`

## 架构与入口

- 模块 `github.com/ndsky1003/task`，Go 1.26。
- 唯一入口 `NewTaskMgr(serialize.ITaskSerialize, IOperator, ...*Option)`（`mgr.go`）。参数为 nil 会 `panic`。
- **无需用户实现任何接口**，核心类型：
  - `task.Task`（`task/task.go`）：框架字段（ID/Type/UpdateTime/CreateTime/Status/Order）由库管理，业务数据存 `Data []byte`（原始字节）。
  - `task.Meta`：添加任务时的元信息（`ID` 可选、`Type`、`Order`）。
  - `IOperator`（`operator.go`，task 包内）：`HandleTask(*task.Task) error`。
  - `IContextOperator`（`operator.go`，可选接口）：`HandleTaskCtx(ctx, *task.Task) error`，实现后库优先调用，配合 `Option.Context`/`Option.TaskTimeout` 支持超时与取消。
- 添加任务：`mgr.Add(data []byte, meta task.Meta) error`（序列化/反序列化交给调用者，ID 为空则库生成）。
- 可观测性：`mgr.Stats()`（`metrics.go`）返回 `Added/Handled/Failed/Dead/Handling` 统计；`Option.SetOnDead` 设置死信回调。
- 内置序列化实现（构造函数会建表/索引，失败即 `panic`）：
  - `serialize.Mongo(client, database, coll)`（`serialize/mongo.go`），需真实可用的 MongoDB。
  - `serialize.Sqlite(db, table)`（`serialize/sqlite.go`），`db` 由调用方用任意 SQLite driver（推荐纯 Go 的 `modernc.org/sqlite`）打开。
- 二者都实现可选接口 `serialize.IDeadLetter`（`serialize/deadletter.go`），`task_mgr` 通过类型断言启用：重试耗尽后将任务置为 `Dead`。

## 关键陷阱

- **业务数据由调用者序列化为 `[]byte`**：格式不限（JSON、protobuf、gob 等）；消费时在 `HandleTask` 里按 `Type` 自行反序列化，类型匹配靠 `Type` 约定而非编译器强制。
- **主键为 `string`**：`Meta.ID` 为空则库用 UUID v4（`google/uuid`）生成；自定义须保证唯一。
- 普通任务与有序任务使用**独立并发控制**：`ConcurrenceNum`（普通任务并发）与 `OrderConcurrenceNum`（有序任务并发，即同时处理的有序类型数）互不影响，有序任务不会饿死普通任务（见 `mgr.go` 顶部注释与 `handleOrderTasks`）。
- 有序任务（`Order==true`）同 `Type` 按 `CreateTime` 顺序串行执行；出错按 `OrderTaskHandleDelta` 递增重试。达到最大尝试次数 `OrderTaskMaxRetry`（默认 8，见 `option.go`）后任务标记为**死信（`Dead`）**并放弃（`mgr.go` `handleOrderTask`），不会阻塞同类型其他任务。
- 处理成功的任务会被直接删除（`Remove`），不存在「完成」状态残留；删除失败会按 `NormalTaskHandleDelta` 自动重试。
- 用户处理函数 `HandleTask` 的 `panic` 会被 `handleTaskSafely` 捕获并转为失败重试，不会拖垮进程。
- 优雅关闭用 `Shutdown()`（`mgr.go`）：停止循环并等待在途任务完成；关闭后 `Add` 返回错误。

## 约定

- 代码注释为中文，新增代码沿用中文注释风格。
- 日志使用标准库 `log/slog`（`mgr.go`）。
