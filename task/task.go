package task

import (
	"errors"
	"time"

	"github.com/ndsky1003/task/taskstatus"
)

var ErrNoTask = errors.New("ErrNoTask")

// Task 任务：框架字段由库管理，业务数据以原始字节存储，序列化/反序列化交给调用者。
type Task struct {
	ID         string       `bson:"_id" json:"id"`
	Type       uint32       `bson:"Type" json:"type"`
	UpdateTime time.Time    `bson:"UpdateTime" json:"update_time"`
	CreateTime time.Time    `bson:"CreateTime" json:"create_time"`
	Status     taskstatus.T `bson:"Status" json:"status"`
	Order      bool         `bson:"Order" json:"order"`
	Data       []byte       `bson:"Data" json:"data"` // 业务数据（原始字节）
}

// Meta 任务元信息，Add 时指定。
type Meta struct {
	ID    string // 可选，为空则库生成
	Type  uint32
	Order bool
	// 未来可扩展：Delay、Priority、MaxRetry ...
}
