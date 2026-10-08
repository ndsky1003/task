# task

任务循环（RunLoop）库，任务持久化到 MongoDB。支持并发处理普通任务，以及同一类型按添加顺序串行执行的有序任务。

## 特性

- 任务持久化到 MongoDB，进程重启自动恢复（启动时将遗留的「处理中」任务还原为初始状态）
- 普通任务并发处理，并发数可配置
- 有序任务（`IsOrder()==true`）同一 `Type` 按 `CreateTime` 顺序串行执行
- 处理失败自动重试，支持递增重试间隔
- 纯 Go 库，无 `main` 包，通过 `NewTaskMgr` 集成

## 安装

```
go get github.com/ndsky1003/task
```

## 快速开始

需要实现三个接口：`itask.ITask`（任务）、`operator.IOperator`（处理逻辑）、`serialize.ITaskSerialize`（存储，可用内置的 `serialize.Mongo`）。

```go
package main

import (
	"context"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ndsky1003/task"
	"github.com/ndsky1003/task/itask"
	"github.com/ndsky1003/task/operator"
	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/taskstatus"
)

// 1. 定义任务，实现 itask.ITask
type MyTask struct {
	ID         primitive.ObjectID `bson:"_id"` // 必须为 _id，作为主键
	Type       uint32             `bson:"Type"`
	UpdateTime time.Time          `bson:"UpdateTime"`
	CreateTime time.Time          `bson:"CreateTime"`
	Status     taskstatus.T       `bson:"Status"`
	Order      bool               `bson:"Order"`

	Payload string `bson:"Payload"` // 业务字段
}

func (t *MyTask) GetID() any               { return t.ID }
func (t *MyTask) GetUpdateTime() time.Time { return t.UpdateTime }
func (t *MyTask) GetCreateTime() time.Time { return t.CreateTime }
func (t *MyTask) SetUpdateTime(a time.Time) { t.UpdateTime = a }
func (t *MyTask) SetCreateTime(a time.Time) { t.CreateTime = a }
func (t *MyTask) GetType() uint32           { return t.Type }
func (t *MyTask) IsOrder() bool             { return t.Order }

// 2. 实现 operator.IOperator
type MyOperator struct{}

func (o *MyOperator) HandleTask(t itask.ITask) error {
	task := t.(*MyTask)
	fmt.Println("handle:", task.Payload)
	return nil // 返回 nil 表示处理成功，任务会被删除
}

func main() {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		panic(err)
	}

	// 3. 创建序列化器（需要真实可用的 MongoDB 连接）
	ser := serialize.Mongo[MyTask](client, "mydb", "tasks")

	// 4. 创建任务管理器，可选配置
	opt := task.Option()
	opt.SetConcurrenceNum(20)

	mgr := task.NewTaskMgr(ser, &MyOperator{}, opt)

	// 5. 添加任务
	mgr.Add(&MyTask{
		ID:      primitive.NewObjectID(),
		Type:    1,
		Payload: "hello",
	})

	time.Sleep(time.Second)
}
```

## 配置

通过 `task.Option()` 获取配置并设置：

| 方法 | 默认值 | 说明 |
| --- | --- | --- |
| `SetConcurrenceNum(n)` | 10（上限 1000） | 最大并发数 |
| `SetNormalTaskHandleDelta(d)` | 1s | 普通任务处理失败后的重试间隔 |
| `SetOrderTaskHandleDelta(s)` | 递增序列 | 有序任务处理失败后的重试间隔序列 |
| `SetOrderLockTTL(d)` | 10m | 有序任务跨实例锁的租约时长，应大于处理单个有序类型的最长耗时 |
| `SetOrderLockRetryDelta(d)` | 5s | 有序任务锁被其他实例占用时的退避时间 |

## 优雅关闭

调用 `mgr.Shutdown()` 停止任务循环并等待所有正在处理的任务完成。关闭后 `Add` 返回错误；已持久化的任务保留在存储中，进程重启后自动恢复继续处理。

## 日志

使用标准库 `log/slog` 输出结构化日志（`Info`/`Error` 级别），默认输出到标准输出。

## 语义保证

- **至少一次（at-least-once）**：进程崩溃时，正在处理（`Handling`）的任务会在下次启动时被还原为初始状态并重新处理，因此业务处理逻辑应保持幂等。
- **普通任务**：并发处理，失败按 `NormalTaskHandleDelta` 重试。
- **有序任务**：同一 `Type` 严格按 `CreateTime` 顺序串行执行；失败按 `OrderTaskHandleDelta` 递增重试，重试耗尽（255 次）后任务被标记为**死信（`Dead`）**，不再自动重试，需人工介入。

## 多实例部署

`serialize.Mongo` 内置了有序任务的**跨实例锁**：多个进程共享同一 MongoDB 时，同一类型的有序任务仍只会被一个实例串行处理。锁基于 `_id` 唯一索引 + 租约过期抢占实现，实例崩溃后锁在租约（`OrderLockTTL`）到期后自动释放。

## 注意事项

- **并发数必须大于有序任务类型数**：否则有序任务会占满并发槽，普通任务无法执行。
- **`ID` 字段必须用 `bson:"_id"` 标签**：删除与状态还原都按 `_id` 匹配，标签错误会导致任务删除/重试失败。
- **`serialize.Mongo[T]` 要求 `*T` 实现 `itask.ITask`**：仅在 `Next`/`NextByType` 里做运行时类型断言，编译期不校验，断言失败返回 error。
- **处理成功的任务会直接删除**，不存在「完成」状态残留；删除失败会自动重试，不会静默丢失。
- **用户处理函数 `panic` 会被捕获**并转为失败重试，不会拖垮进程。
- **`NewTaskMgr` 会在构造函数里建索引**，索引创建失败会 `panic`，需保证 MongoDB 连接可用。
