# task

任务循环（RunLoop）库，任务持久化到 MongoDB 或 SQLite。支持并发处理普通任务，以及同一类型按添加顺序串行执行的有序任务。

## 特性

- 任务持久化到 MongoDB / SQLite，进程重启自动恢复（启动时将遗留的「处理中」任务还原为初始状态）
- 普通任务并发处理，并发数可配置
- 有序任务（`Order=true`）同一 `Type` 按 `CreateTime` 顺序串行执行
- 处理失败自动重试，支持递增重试间隔
- **无需实现任何接口**：框架字段（ID/类型/时间/状态/顺序）由库内置管理，用户只需定义业务数据结构
- 一个 `task_mgr` 天然支持**任意多类型任务**（按 `Type` 分发、反序列化）
- 纯 Go 库，无 `main` 包，通过 `NewTaskMgr` 集成

## 安装

```
go get github.com/ndsky1003/task
```

## 快速开始

### 1. 定义业务数据与处理逻辑

用户只需定义**业务数据结构**，无需实现任何接口；处理时按 `Type` 分发并反序列化：

```go
package main

import (
	"encoding/json"

	"github.com/ndsky1003/task/task"
)

// 业务数据：不同任务类型不同的结构
type OrderPayload struct {
	OrderID string `json:"order_id"`
	Amount  int    `json:"amount"`
}

type RechargePayload struct {
	UserID string `json:"user_id"`
	Points int    `json:"points"`
}

// 处理逻辑：一个 operator 处理所有类型，按 Type 分发并自行反序列化
type MyOperator struct{}

func (o *MyOperator) HandleTask(t *task.Task) error {
	switch t.Type {
	case 100: // 订单
		var p OrderPayload
		if err := json.Unmarshal(t.Data, &p); err != nil {
			return err
		}
		_ = p.OrderID
	case 200: // 充值
		var r RechargePayload
		if err := json.Unmarshal(t.Data, &r); err != nil {
			return err
		}
		_ = r.Points
	}
	return nil // 返回 nil 表示处理成功，任务会被删除；返回 error 则按配置重试
}
```

### 2. 使用 MongoDB 存储

```go
import (
	"context"
	"encoding/json"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	taskmgr "github.com/ndsky1003/task" // 根目录：NewTaskMgr
	"github.com/ndsky1003/task/task"    // 子目录：Task, Meta
	"github.com/ndsky1003/task/serialize"
)

func main() {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		panic(err)
	}

	ser := serialize.Mongo(client, "mydb", "tasks") // 构造函数会建索引，需真实可用的 MongoDB
	mgr := taskmgr.NewTaskMgr(ser, &MyOperator{})

	// 添加任务：序列化交给调用者，传入业务数据字节 + 元信息（类型、是否有序）
	data, _ := json.Marshal(OrderPayload{OrderID: "1", Amount: 100})
	if err := mgr.Add(data, task.Meta{Type: 100}); err != nil {
		panic(err)
	}
}
```

### 3. 使用 SQLite 存储

单实例场景推荐，零外部依赖、单文件，无需起数据库服务：

```go
import (
	"database/sql"
	"encoding/json"

	_ "modernc.org/sqlite" // 纯 Go 驱动，任意 SQLite driver 均可

	taskmgr "github.com/ndsky1003/task" // 根目录：NewTaskMgr
	"github.com/ndsky1003/task/task"    // 子目录：Task, Meta
	"github.com/ndsky1003/task/serialize"
)

func main() {
	db, err := sql.Open("sqlite", "file:tasks.db")
	if err != nil {
		panic(err)
	}
	db.SetMaxOpenConns(1) // SQLite 单写者，建议限制连接数为 1

	// 可选优化：WAL + 降低同步级别，写入吞吐提升约 6 倍（崩溃时最多丢失最近约 1 秒的写入）
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;"); err != nil {
		panic(err)
	}

	ser := serialize.Sqlite(db, "tasks") // 构造函数会建表与索引
	mgr := taskmgr.NewTaskMgr(ser, &MyOperator{})

	data, _ := json.Marshal(OrderPayload{OrderID: "1", Amount: 100})
	if err := mgr.Add(data, task.Meta{Type: 100}); err != nil {
		panic(err)
	}
}
```

> `serialize.Sqlite` 将任务整体以 JSON 存储，`id`/`type`/`status`/时间拆为独立列用于查询、排序与状态机。

## 有序任务

`Meta.Order = true` 的任务为有序任务，同一 `Type` 严格按 `CreateTime` 顺序串行执行（不同类型可并行）。典型场景如充值、转账等需要按提交顺序处理的任务：

```go
// 同一类型（Type=100）的多个任务，将按添加顺序依次处理
d1, _ := json.Marshal(OrderPayload{OrderID: "1", Amount: 100})
d2, _ := json.Marshal(OrderPayload{OrderID: "2", Amount: 200})
mgr.Add(d1, task.Meta{Type: 100, Order: true})
mgr.Add(d2, task.Meta{Type: 100, Order: true})
```

## 优雅关闭

```go
// 停止任务循环并等待所有正在处理的任务完成。
// 关闭后 Add 返回错误；已持久化的任务保留在存储中，进程重启后自动恢复继续处理。
mgr.Shutdown()
```

## 配置

通过 `taskmgr.Options()`（根目录 task 包）获取配置并设置：

| 方法 | 默认值 | 说明 |
| --- | --- | --- |
| `SetConcurrenceNum(n)` | 10（上限 1000） | 最大并发数 |
| `SetNormalTaskHandleDelta(d)` | 1s | 普通任务处理失败后的重试间隔 |
| `SetOrderTaskHandleDelta(s)` | 递增序列 | 有序任务处理失败后的重试间隔序列 |
| `SetOrderTaskMaxRetry(n)` | 8 | 有序任务处理失败后的最大尝试次数（含首次），超出则标记死信 |

## 性能对比

本机实测（Apple Silicon、本地 MongoDB、固定迭代 `-benchtime=2000x`/`1000x`，`go test -bench=. -benchmem ./serialize/`）：

| 操作 | Sqlite 内存 | Sqlite 文件(WAL) | Sqlite 文件(默认) | MongoDB |
| --- | --- | --- | --- | --- |
| Add（写入） | 20 µs | 41 µs | 333 µs | 185 µs |
| Next（取任务） | 250 µs | — | 617 µs | 925 µs |
| 完整生命周期 | 46 µs | 110 µs | 846 µs | 936 µs |

说明：

- **Sqlite 内存**：`:memory:`，不持久化，最快。
- **Sqlite 文件(WAL)**：`PRAGMA journal_mode=WAL; synchronous=NORMAL`，推荐配置。
- **Sqlite 文件(默认)**：默认 `synchronous=FULL`（每次写都 fsync）。
- **完整生命周期** = Add + Next + Remove，最贴近真实使用。

结论：

- **单实例场景 SQLite 全面更快**：开 WAL 后完整生命周期约 110 µs，比 MongoDB（936 µs）快约 **8 倍**。
- 唯一拖慢 SQLite 的点是默认 `synchronous=FULL` 的每次 fsync（Add 333 µs），改成 WAL 后降到 41 µs。
- 本质原因：SQLite 无网络往返，MongoDB 每个操作都走网络（`Next` 是 `FindOne`+`UpdateOne` 两次往返）。

> 这是单实例、单连接（`SetMaxOpenConns(1)`）、本地 MongoDB 的结果。MongoDB 的多连接并发写、副本集、水平扩展优势在此场景用不上；真实业务里若任务处理本身有毫秒到秒级耗时，存储层差异会被稀释。

## 日志

使用标准库 `log/slog` 输出结构化日志（`Info`/`Error` 级别），默认输出到标准输出。

## 语义保证

- **至少一次（at-least-once）**：进程崩溃时，正在处理（`Handling`）的任务会在下次启动时被还原为初始状态并重新处理，因此业务处理逻辑应保持幂等。
- **普通任务**：并发处理，失败按 `NormalTaskHandleDelta` 重试。
- **有序任务**：同一 `Type` 严格按 `CreateTime` 顺序串行执行；失败按 `OrderTaskHandleDelta` 递增重试，达到最大尝试次数（`OrderTaskMaxRetry`，默认 8 次）后任务被标记为**死信（`Dead`）**，不再自动重试，需人工介入。

## 注意事项

- **并发数必须大于有序任务类型数**：否则有序任务会占满并发槽，普通任务无法执行。
- **业务数据由调用者序列化为 `[]byte`**：格式不限（JSON、protobuf、gob 等）；消费时在 `HandleTask` 里按 `Type` 自行反序列化。
- **任务主键为 `string`**：`Meta.ID` 为空时由库生成随机 ID；如需自定义，须保证唯一。
- **处理成功的任务会直接删除**，不存在「完成」状态残留；删除失败会自动重试，不会静默丢失。
- **用户处理函数 `panic` 会被捕获**并转为失败重试，不会拖垮进程。
- **`NewTaskMgr` 会在构造函数里建表/索引**，创建失败会 `panic`，需保证存储连接可用。
