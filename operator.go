package task

import "github.com/ndsky1003/task/task"

// IOperator 任务处理逻辑接口。
type IOperator interface {
	// HandleTask 处理任务，返回 nil 表示成功（任务会被删除），返回 error 表示失败（按配置重试）。
	HandleTask(task *task.Task) error
}
