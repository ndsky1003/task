# 新 API 设计草案（历史文档）

> **本文是早期设计草案，已多次演进，部分内容（如 `json.RawMessage`、`Add(data any)`、`UnmarshalData`）已过时。**
> **当前准确用法与结构以 `README.md` 为准。**
> 最终落地要点：`Data` 为 `[]byte`（序列化交给调用者）、`Add(data []byte, meta)`、主键用 UUID v4、Task 定义在 `task/` 子目录。

## 设计动机

现有方案要求用户为每个任务实现 `task.ITask` 接口的 7 个方法，且 `serialize.Mongo[T]`/`Sqlite[T]` 的泛型 `T` 只承担「反序列化目标」一个职责，类型安全仍靠运行时断言，且一个 `task_mgr` 只能处理一种数据类型。

新方案：

- 框架字段（ID/Type/时间/状态/Order）由库内置管理，业务数据以 JSON 原始字节存储。
- 抹掉 `task.ITask` 接口，用户无需再实现任何 getter/setter。
- `Task` 非泛型，`Data` 为 `json.RawMessage`，消费时按 `Type` 反序列化 → **一个 `task_mgr` 天然支持任意多类型任务**。
- 反序列化点用泛型辅助方法 `DataAs[T]` 找回编译期类型安全。

## 包结构（最终落地）

```
（根目录，package task）  mgr.go       // task_mgr
                          operator.go  // IOperator
                          option.go
task/                     task.go      // Task 结构 + Meta + UnmarshalData（独立子目录，无反向依赖）
serialize/                i_serialize.go // ITaskSerialize、IDeadLetter 接口（*task.Task）
                          mongo.go
                          sqlite.go
                          deadletter.go
taskstatus/               enum.go
taskmgrstatus/            t.go
```

依赖方向：根目录 `task` → `task/`（子目录）+ `serialize`；`serialize` → `task/`（子目录）。无环。
（Task 定义独立为 `task/` 子目录，使 `serialize` 只依赖子目录、不依赖根目录，避免循环依赖。）

## 核心类型

### task/task.go

```go
package itask

import (
	"encoding/json"
	"time"

	"github.com/ndsky1003/task/taskstatus"
)

// Task 任务：框架字段由库管理，业务数据以 JSON 原始字节存储。
type Task struct {
	ID         string          `bson:"_id" json:"id"`
	Type       uint32          `bson:"Type" json:"type"`
	UpdateTime time.Time       `bson:"UpdateTime" json:"update_time"`
	CreateTime time.Time       `bson:"CreateTime" json:"create_time"`
	Status     taskstatus.T    `bson:"Status" json:"status"`
	Order      bool            `bson:"Order" json:"order"`

	Data json.RawMessage `bson:"Data" json:"data"`
}

// DataAs 将业务数据反序列化为具体类型 T（反序列化点的编译期安全）。
func (t *Task) DataAs[T any]() (*T, error) {
	var v T
	if err := json.Unmarshal(t.Data, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// Meta 任务元信息，Add 时指定。
type Meta struct {
	ID    string // 可选，为空则库生成
	Type  uint32
	Order bool
	// 未来可扩展：Delay、Priority、MaxRetry ...
}
```

### task/operator.go（IOperator 移入 task 包）

```go
package task

import "github.com/ndsky1003/task/task"

type IOperator interface {
	HandleTask(task *task.Task) error
}
```

### serialize/i_serialize.go

```go
package serialize

import "github.com/ndsky1003/task/task"

type ITaskSerialize interface {
	Recover() error
	Add(*task.Task) error
	Next(exclude ...uint32) (*task.Task, error)
	NextByType(t uint32) (*task.Task, error)
	Remove(*task.Task) error
	UpdateStatus2Init(*task.Task) error
	HasNext(exclude ...uint32) (bool, error)
}
```

### task/mgr.go（关键方法签名）

```go
package task

func NewTaskMgr(ser serialize.ITaskSerialize, op IOperator, opts ...*Option) *task_mgr

// Add 添加任务：data 为业务数据（任意结构体），内部 JSON 序列化；meta 指定类型与是否有序。
func (m *task_mgr) Add(data any, meta task.Meta) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	id := meta.ID
	if id == "" {
		id = newID() // crypto/rand 生成，库管理
	}
	task := &task.Task{ID: id, Type: meta.Type, Order: meta.Order, Data: b}
	return m.add(task)
}
```

其余 `run_loop` / 并发槽 / 有序任务串行 / panic 兜底 / 失败重试 / 死信 / 优雅关闭等逻辑**保持不变**，只是把 `task.ITask` 接口全部替换为 `*task.Task`。

## 完整最小示例（多类型任务）

```go
package main

import (
	"database/sql"

	_ "modernc.org/sqlite"

	"github.com/ndsky1003/task"
	"github.com/ndsky1003/task/task"
	"github.com/ndsky1003/task/serialize"
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

// 处理逻辑：一个 operator 处理所有类型，按 Type 分发
type MyOperator struct{}

func (o *MyOperator) HandleTask(t *task.Task) error {
	switch t.Type {
	case 100: // 订单
		p, err := t.DataAs[OrderPayload]()
		if err != nil {
			return err
		}
		_ = p.OrderID // 编译期即 OrderPayload
	case 200: // 充值
		r, err := t.DataAs[RechargePayload]()
		if err != nil {
			return err
		}
		_ = r.Points
	}
	return nil
}

func main() {
	db, _ := sql.Open("sqlite", "file:tasks.db")
	db.SetMaxOpenConns(1)

	ser := serialize.Sqlite(db, "tasks")
	mgr := task.NewTaskMgr(ser, &MyOperator{})

	// 一个 manager 添加多种类型的任务
	_ = mgr.Add(OrderPayload{OrderID: "1", Amount: 100}, task.Meta{Type: 100})
	_ = mgr.Add(RechargePayload{UserID: "u1", Points: 50}, task.Meta{Type: 200, Order: true})
}
```

## 与现状的差异（迁移影响）

| 维度 | 现状 | 新方案 |
| --- | --- | --- |
| 用户实现 | 结构体 + 7 个方法 + 双份 tag | 只定义业务数据 struct |
| 类型安全 | 运行时断言 | `DataAs[T]` 编译期 |
| 多类型 | 一个 `task_mgr` 一种 `T` | 一个 `task_mgr` 任意类型 |
| 框架字段 tag | 用户手写 | 库内置 |
| 主键 | `any`（ObjectID/string…） | 固定 `string`（库生成 UUID） |
| 业务数据访问 | `task.Payload` | `task.DataAs[T]()` 或 `task.Data` |

## 待确认的决策点

1. **主键固定为 `string`**：是否接受放弃 `ObjectID` 等自定义主键？
2. **`IOperator` 移入 `task` 包**（原 `operator` 包删除）：是否接受包结构调整？
3. **`Data` 用 `json.RawMessage`**：内部 JSON 序列化，是否接受（对比 `any` 更明确，但需用户数据可 JSON 序列化）？
