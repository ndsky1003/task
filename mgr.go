package task

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/samber/lo"

	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/task"
	"github.com/ndsky1003/task/taskmgrstatus"
)

type handleStatusReq struct {
	status taskmgrstatus.T // Start 或 Stop
	meta   *task.Task      // 仅 status==Start 时可选：需要先还原状态再重试的任务
}

type task_mgr struct {
	l                         sync.Mutex
	status                    atomic.Uint32
	handling_order_task_types []uint32 // 正在执行的 OrderTask 的 Type
	task_serialize            serialize.ITaskSerialize
	_handle_task_operator     IOperator
	done                      chan struct{}
	concurrenceNum            chan struct{} // 普通任务限流
	orderConcurrenceNum       chan struct{} // 有序任务限流（同时处理的有序类型数上限）
	opt                       *Option
	shutdown                  atomic.Bool
	runLoopWg                 sync.WaitGroup // 跟踪 run_loop，保证优雅关闭时不再新增任务 goroutine
	wg                        sync.WaitGroup // 跟踪任务处理 goroutine

	// metrics
	added    atomic.Uint64 // 累计添加的任务数
	handled  atomic.Uint64 // 累计成功处理数
	failed   atomic.Uint64 // 累计失败次数（含重试）
	dead     atomic.Uint64 // 累计死信数
	handling atomic.Int64  // 当前处理中的任务数
}

// newID 生成任务主键（UUID v4）。
func newID() string {
	return uuid.NewString()
}

// NewTaskMgr 创建任务管理器并启动任务循环。
// 普通任务与有序任务使用独立的并发控制（分别由 ConcurrenceNum、OrderConcurrenceNum 限制），互不阻塞。
func NewTaskMgr(task_serialize serialize.ITaskSerialize, op IOperator, opts ...*Option) *task_mgr {
	if task_serialize == nil {
		panic("task_serialize must not nil")
	}
	if op == nil {
		panic("op must not nil")
	}
	opt := Options().Merge(opts...)

	c := &task_mgr{
		concurrenceNum:        make(chan struct{}, opt.ConcurrenceNum),
		orderConcurrenceNum:   make(chan struct{}, opt.OrderConcurrenceNum),
		task_serialize:        task_serialize,
		_handle_task_operator: op,
		opt:                   opt,
	}
	if err := task_serialize.Recover(); err != nil {
		panic(err)
	}
	c.Start()
	return c
}

func (this *task_mgr) push_handling_order_task_types(T uint32) {
	this.l.Lock()
	defer this.l.Unlock()
	if !lo.Contains(this.handling_order_task_types, T) {
		this.handling_order_task_types = append(this.handling_order_task_types, T)
	}
}

func (this *task_mgr) get_handling_order_task_types() []uint32 {
	this.l.Lock()
	defer this.l.Unlock()
	r := make([]uint32, len(this.handling_order_task_types))
	copy(r, this.handling_order_task_types)
	return r
}

func (this *task_mgr) pop_handling_order_task_types(T uint32) {
	this.l.Lock()
	defer this.l.Unlock()
	if index := lo.IndexOf(this.handling_order_task_types, T); index != -1 {
		this.handling_order_task_types = append(this.handling_order_task_types[:index], this.handling_order_task_types[index+1:]...)
	}
}

func (this *task_mgr) Start() {
	this.handleStatus(&handleStatusReq{status: taskmgrstatus.Start})
}

func (this *task_mgr) Stop() bool {
	return this.handleStatus(&handleStatusReq{status: taskmgrstatus.Stop})
}

// Shutdown 优雅关闭：停止任务循环并等待所有正在处理的任务完成。
// 关闭后 Add 返回错误；已持久化的任务保留在存储中，进程重启后由 Recover 还原继续处理。
func (this *task_mgr) Shutdown() {
	this.shutdown.Store(true)
	this.l.Lock()
	if this.done != nil {
		close(this.done)
		this.done = nil
	}
	this.l.Unlock()
	this.runLoopWg.Wait() // 等 run_loop 退出，确保不再新增处理 goroutine
	this.wg.Wait()        // 等正在处理的任务完成
}

// handleStatus 处理启动/停止，全程加锁以避免并发关闭 done。
// 返回值仅对 Stop 有意义：true 表示任务循环已停止运行。
func (this *task_mgr) handleStatus(req *handleStatusReq) bool {
	this.l.Lock()
	defer this.l.Unlock()
	switch req.status {
	case taskmgrstatus.Start:
		if this.shutdown.Load() {
			return false
		}
		// 若是出错重试（meta 携带任务），先还原任务状态
		if req.meta != nil {
			if !this.updateStatus2InitWithRetry(req.meta) {
				return false
			}
		}
		if !this.status.CompareAndSwap(taskmgrstatus.Stop, taskmgrstatus.Start) {
			return false
		}
		if this.done != nil {
			close(this.done)
			this.done = nil
		}
		this.done = make(chan struct{}, 1)
		this.runLoopWg.Add(1)
		go this.run_loop(this.done)
		return false
	case taskmgrstatus.Stop:
		// 二次检查兜底竞态：Add 先写库再 Start。若 run_loop 恰在 Add 写库后、Start 之前 Next 返回 task.ErrNoTask，
		// Start 的 CAS(Stop,Start) 会因 status 仍为 Start 而失败返回（不重启），此时由这里重新 HasNext，
		// 读到刚写入的任务则返回 false 继续循环，避免任务丢失。此分支与 Start 同锁，二者互斥。
		b, err := this.task_serialize.HasNext(this.handling_order_task_types...)
		if err != nil {
			slog.Error("has next failed", "err", err)
			return false
		}
		if b {
			return false
		}
		if !this.status.CompareAndSwap(taskmgrstatus.Start, taskmgrstatus.Stop) {
			return true
		}
		if this.done != nil {
			close(this.done)
			this.done = nil
		}
		return true
	}
	return false
}

// Add 添加任务：data 为业务数据的原始字节（序列化交给调用者），meta 指定类型与是否有序。
func (this *task_mgr) Add(data []byte, meta task.Meta) error {
	id := meta.ID
	if id == "" {
		id = newID()
	}
	return this.add(&task.Task{
		ID:    id,
		Type:  meta.Type,
		Order: meta.Order,
		Data:  data,
	})
}

func (this *task_mgr) add(task *task.Task) error {
	if this.shutdown.Load() {
		return errors.New("task manager is shutdown")
	}
	now := time.Now()
	if is_zero_time(task.UpdateTime) {
		task.UpdateTime = now
	}
	if is_zero_time(task.CreateTime) {
		task.CreateTime = now
	}
	err := this.task_serialize.Add(task)
	if err == nil {
		this.added.Add(1)
		this.Start()
	}
	return err
}

func (this *task_mgr) run_loop(done chan struct{}) {
	slog.Info("start runloop")
	defer func() {
		this.runLoopWg.Done()
		slog.Info("runloop done")
	}()
	for {
		select {
		case <-done:
			return
		default:
		}

		t, err := this.task_serialize.Next(this.get_handling_order_task_types()...)
		if err == nil {
			if t.Order {
				// 有序任务在 goroutine 内占槽，避免阻塞 run_loop 获取普通任务
				this.wg.Add(1)
				this.push_handling_order_task_types(t.Type)
				go this.handleOrderTasks(t)
			} else {
				this.wg.Add(1)
				this.concurrenceNum <- struct{}{}
				go this.handleTask(t)
			}
			continue
		}
		if err == task.ErrNoTask {
			// 无任务则尝试停止；若已停止则退出循环
			if this.Stop() {
				return
			}
			continue
		}
		slog.Info("next task failed", "err", err)
		time.Sleep(time.Second)
	}
}

// handleTask 处理普通任务：带 panic 兜底，失败重试，成功删除（删除失败自动重试）。
func (this *task_mgr) handleTask(task *task.Task) {
	defer func() {
		<-this.concurrenceNum
		this.wg.Done()
	}()
	if err := this.handleTaskSafely(task); err != nil {
		this.failed.Add(1)
		time.AfterFunc(this.opt.NormalTaskHandleDelta, func() {
			this.handleStatus(&handleStatusReq{
				status: taskmgrstatus.Start,
				meta:   task, // 出错重试：还原状态后重新入队
			})
		})
	} else { // 处理成功，删除任务
		this.handled.Add(1)
		this.removeWithRetry(task)
	}
}

// handleTaskSafely 调用用户处理函数并捕获 panic，将 panic 转为 error，避免进程崩溃。
func (this *task_mgr) handleTaskSafely(task *task.Task) (err error) {
	this.handling.Add(1)
	defer func() {
		this.handling.Add(-1)
		if r := recover(); r != nil {
			slog.Error("handle task panic", "task", task.ID, "panic", r)
			err = fmt.Errorf("task panic: %v", r)
		}
	}()

	// 优先使用支持 context 的 operator
	if op, ok := this._handle_task_operator.(IContextOperator); ok {
		ctx, cancel := this.taskContext()
		defer cancel()
		return op.HandleTaskCtx(ctx, task)
	}
	return this._handle_task_operator.HandleTask(task)
}

// taskContext 派生任务处理的 context：base context + 可选超时。
func (this *task_mgr) taskContext() (context.Context, context.CancelFunc) {
	ctx := this.opt.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if this.opt.TaskTimeout > 0 {
		return context.WithTimeout(ctx, this.opt.TaskTimeout)
	}
	return ctx, func() {}
}

// removeWithRetry 删除任务；失败按 NormalTaskHandleDelta 延迟重试，避免任务永久卡在 Handling。
func (this *task_mgr) removeWithRetry(task *task.Task) {
	if err := this.task_serialize.Remove(task); err != nil {
		slog.Error("remove task failed, will retry", "task", task.ID, "err", err)
		time.AfterFunc(this.opt.NormalTaskHandleDelta, func() {
			this.removeWithRetry(task)
		})
	}
}

// updateStatus2InitWithRetry 还原任务状态；失败延迟重试。返回本次是否成功。
func (this *task_mgr) updateStatus2InitWithRetry(task *task.Task) bool {
	if err := this.task_serialize.UpdateStatus2Init(task); err != nil {
		slog.Error("update status to init failed, will retry", "task", task.ID, "err", err)
		time.AfterFunc(this.opt.NormalTaskHandleDelta, func() {
			if this.updateStatus2InitWithRetry(task) {
				this.Start() // 还原成功，唤醒任务循环继续处理
			}
		})
		return false
	}
	return true
}

// sleepOrder 有序任务重试前按递增序列休眠。attempt 从 1 开始。
func (this *task_mgr) sleepOrder(attempt uint32) {
	loopLength := len(this.opt.OrderTaskHandleDelta)
	if loopLength == 0 {
		time.Sleep(time.Second)
		return
	}
	si := int(attempt-1) % loopLength
	time.Sleep(this.opt.OrderTaskHandleDelta[si])
}

// handleOrderTasks 串行处理某类型的所有有序任务：
// 先处理传入的第一个，再按 CreateTime 顺序处理该类型的其余任务，全部完成后释放有序并发槽。
func (this *task_mgr) handleOrderTasks(first *task.Task) {
	t := first.Type
	this.orderConcurrenceNum <- struct{}{} // 占有序并发槽（在 goroutine 内阻塞，不影响 run_loop）
	slog.Info("handleOrderTasks", "type", t)
	dead := false
	defer func() {
		this.pop_handling_order_task_types(t)
		slog.Info("defer handleOrderTasks", "type", t)
		<-this.orderConcurrenceNum
		this.wg.Done()
		if dead {
			// 当前任务被标记死信，但可能还有同类型任务未处理，唤醒 run_loop 继续
			this.Start()
		}
	}()

	if dead = this.handleOrderTask(first); dead {
		return
	}

	for {
		next, err := this.task_serialize.NextByType(t)
		if err != nil {
			if err == task.ErrNoTask {
				return
			}
			slog.Error("next by type failed", "err", err)
			time.Sleep(time.Second)
			continue
		}
		if dead = this.handleOrderTask(next); dead {
			return
		}
	}
}

// handleOrderTask 处理单个有序任务：带 panic 兜底与重试，重试耗尽则标记死信并返回 true。
func (this *task_mgr) handleOrderTask(task *task.Task) (dead bool) {
	maxRetry := this.opt.OrderTaskMaxRetry
	for attempt := uint32(1); ; attempt++ {
		err := this.handleTaskSafely(task)
		if err == nil {
			this.handled.Add(1)
			this.removeWithRetry(task)
			return false
		}
		this.failed.Add(1)
		if attempt >= maxRetry {
			slog.Error("order task dead", "type", task.Type, "attempt", attempt, "err", err)
			// 标记死信，避免任务永久卡死或阻塞同类型任务；不再自动重试
			if dl, ok := this.task_serialize.(serialize.IDeadLetter); ok {
				if err1 := dl.MarkDead(task); err1 != nil {
					slog.Error("mark dead failed", "task", task.ID, "err", err1)
				}
			}
			this.dead.Add(1)
			if this.opt.OnDead != nil {
				this.opt.OnDead(task, err)
			}
			return true
		}
		this.sleepOrder(attempt)
	}
}
