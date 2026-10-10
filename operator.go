package task

import (
	"context"

	"github.com/ndsky1003/task/task"
)

// IOperator 任务处理逻辑接口。
type IOperator interface {
	// HandleTask 处理任务，返回 nil 表示成功（任务会被删除），返回 error 表示失败（按配置重试）。
	HandleTask(task *task.Task) error
}

// IContextOperator 可选接口：支持 context 的任务处理逻辑。
// 实现该接口时，task 库会优先调用 HandleTaskCtx 而非 HandleTask，
// 并传入由 Option.Context / Option.TaskTimeout 派生的 context（可用于超时与取消）。
type IContextOperator interface {
	HandleTaskCtx(ctx context.Context, task *task.Task) error
}
