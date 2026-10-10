package serialize

import "github.com/ndsky1003/task/task"

// ITaskSerialize 任务存储接口，由存储实现（serialize.Mongo / serialize.Sqlite）实现。
type ITaskSerialize interface {
	// Recover 恢复：将异常退出时遗留的「处理中」任务还原为初始状态，避免任务丢失
	Recover() error
	// Add 添加任务
	Add(*task.Task) error
	// Next 下一个任务,按照 UpdateTime 顺序获取，排除某一类任务（有序任务需要按顺序执行）
	// 先添加的先获取，获取后更新 UpdateTime，防止一个任务一直获取，阻塞死任务
	Next(exclude_t ...uint32) (*task.Task, error)
	// NextByType 同类型的下一个任务，按照 CreateTime 顺序获取
	NextByType(t uint32) (*task.Task, error)
	// Remove 删除任务，任务执行完毕需要删除
	Remove(*task.Task) error
	// UpdateStatus2Init 任务状态还原，等待下次执行
	UpdateStatus2Init(*task.Task) error
	// HasNext 判断是否还有待处理任务（排除某些类型）
	HasNext(exclude_t ...uint32) (bool, error)
}

// IDeadLetter 可选接口：任务重试耗尽后标记为死信，等待人工处理。
type IDeadLetter interface {
	MarkDead(*task.Task) error
}
