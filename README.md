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

## 日志

通过 `task.SetLogger(ILogger)` 注入自定义日志实现（`Infof`/`Errf`/`Info`/`Err`），默认输出到标准输出。

## 注意事项

- **并发数必须大于有序任务类型数**：否则有序任务会占满并发槽，普通任务无法执行。
- **`ID` 字段必须用 `bson:"_id"` 标签**：删除与状态还原都按 `_id` 匹配，标签错误会导致任务删除/重试失败。
- **`serialize.Mongo[T]` 要求 `*T` 实现 `itask.ITask`**：仅在 `Next`/`NextByType` 里做运行时类型断言，编译期不校验，断言失败返回 error。
- **处理成功的任务会直接删除**，不存在「完成」状态残留。
- **`NewTaskMgr` 会在构造函数里建索引**，索引创建失败会 `panic`，需保证 MongoDB 连接可用。
