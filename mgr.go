package task

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/samber/lo"

	"github.com/ndsky1003/task/itask"
	"github.com/ndsky1003/task/operator"
	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/taskmgrstatus"
)

type handleStatusReq struct {
	status taskmgrstatus.T
	meta   any
}

type task_mgr struct {
	l                         sync.Mutex
	status                    atomic.Uint32
	handling_order_task_types []uint32 // 正在执行或暂时退避的 OrderTask 的 Type
	task_serialize            serialize.ITaskSerialize
	_handle_task_operator     operator.IOperator
	done                      chan struct{}
	concurrenceNum            chan struct{} // 限流
	opt                       *Option
	instanceID                string
	shutdown                  atomic.Bool
	runLoopWg                 sync.WaitGroup // 跟踪 run_loop，保证优雅关闭时不再新增任务 goroutine
	wg                        sync.WaitGroup // 跟踪任务处理 goroutine
}

// newInstanceID 生成实例唯一标识，用于跨实例有序任务锁的 owner。
func newInstanceID() string {
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("task-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// NewTaskMgr 创建任务管理器并启动任务循环。
// 注意：最大并发数 opt.ConcurrenceNum 必须大于有序任务类型数，
// 否则有序任务会占满并发槽，导致普通任务无法执行。
func NewTaskMgr(task_serialize serialize.ITaskSerialize, op operator.IOperator, opts ...*Option) *task_mgr {
	if task_serialize == nil {
		panic("task_serialize must not nil")
	}
	if op == nil {
		panic("op must not nil")
	}
	opt := Options().Merge(opts...)

	c := &task_mgr{
		concurrenceNum:        make(chan struct{}, opt.ConcurrenceNum),
		task_serialize:        task_serialize,
		_handle_task_operator: op,
		opt:                   opt,
		instanceID:            newInstanceID(),
	}
	if err := task_serialize.Init(); err != nil {
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
// 关闭后 Add 返回错误；已持久化的任务保留在存储中，进程重启后由 Init 还原继续处理。
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

// handleStatus 处理启动/停止/错误还原，全程加锁以避免并发关闭 done。
// 返回值仅对 Stop 有意义：true 表示任务循环已停止运行。
func (this *task_mgr) handleStatus(req *handleStatusReq) bool {
	this.l.Lock()
	defer this.l.Unlock()
	switch req.status {
	case taskmgrstatus.Start, taskmgrstatus.HandleError:
		if this.shutdown.Load() {
			return false
		}
		if req.status == taskmgrstatus.HandleError {
			if task, ok := req.meta.(itask.ITask); ok {
				if !this.updateStatus2InitWithRetry(task) {
					return false
				}
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

// 添加任务
func (this *task_mgr) Add(task itask.ITask) error {
	return this.add(task)
}

func (this *task_mgr) add(task itask.ITask) error {
	if this.shutdown.Load() {
		return errors.New("task manager is shutdown")
	}
	now := time.Now()
	if is_zero_time(task.GetUpdateTime()) {
		task.SetUpdateTime(now)
	}
	if is_zero_time(task.GetCreateTime()) {
		task.SetCreateTime(now)
	}
	err := this.task_serialize.Add(task)
	if err == nil {
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

		task, err := this.task_serialize.Next(this.get_handling_order_task_types()...)
		if err == nil {
			if task.IsOrder() {
				this.handleOrderTask(task)
			} else {
				this.wg.Add(1)
				this.concurrenceNum <- struct{}{}
				go this.handdleTask(task)
			}
			continue
		}
		if err == itask.ErrNoTask {
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

// handleOrderTask 处理有序任务：先尝试跨实例锁，成功则启动串行处理；被占用则退避重试。
func (this *task_mgr) handleOrderTask(task itask.ITask) {
	t := task.GetType()
	if locker, ok := this.task_serialize.(serialize.IOrderLocker); ok {
		acquired, err := locker.TryLockOrderType(t, this.instanceID, this.opt.OrderLockTTL)
		if err != nil {
			// 锁服务异常：降级为仅本实例内串行，不阻塞任务
			slog.Error("try lock order type failed, fallback to local serial", "type", t, "err", err)
			acquired = true
		}
		if !acquired {
			// 锁被其他实例持有：还原任务并退避，避免忙等
			slog.Info("order type locked by other instance, postpone", "type", t)
			this.updateStatus2InitWithRetry(task)
			this.push_handling_order_task_types(t)
			time.AfterFunc(this.opt.OrderLockRetryDelta, func() {
				this.pop_handling_order_task_types(t)
				this.Start()
			})
			return
		}
	}

	this.wg.Add(1)
	this.concurrenceNum <- struct{}{}
	this.push_handling_order_task_types(t)
	go this.handdleTaskByType(task, t)
}

// handdleTask 处理普通任务：带 panic 兜底，失败重试，成功删除（删除失败自动重试）。
func (this *task_mgr) handdleTask(task itask.ITask) {
	defer func() {
		<-this.concurrenceNum
		this.wg.Done()
	}()
	if err := this.handleTaskSafely(task); err != nil {
		time.AfterFunc(this.opt.NormalTaskHandleDelta, func() {
			this.handleStatus(&handleStatusReq{
				status: taskmgrstatus.HandleError,
				meta:   task,
			})
		})
	} else { // 处理成功，删除任务
		this.removeWithRetry(task)
	}
}

// handleTaskSafely 调用用户处理函数并捕获 panic，将 panic 转为 error，避免进程崩溃。
func (this *task_mgr) handleTaskSafely(task itask.ITask) (err error) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("handle task panic", "task", task, "panic", r)
			err = fmt.Errorf("task panic: %v", r)
		}
	}()
	return this._handle_task_operator.HandleTask(task)
}

// removeWithRetry 删除任务；失败按 NormalTaskHandleDelta 延迟重试，避免任务永久卡在 Handling。
func (this *task_mgr) removeWithRetry(task itask.ITask) {
	if err := this.task_serialize.Remove(task); err != nil {
		slog.Error("remove task failed, will retry", "task", task, "err", err)
		time.AfterFunc(this.opt.NormalTaskHandleDelta, func() {
			this.removeWithRetry(task)
		})
	}
}

// updateStatus2InitWithRetry 还原任务状态；失败延迟重试。返回本次是否成功。
func (this *task_mgr) updateStatus2InitWithRetry(task itask.ITask) bool {
	if err := this.task_serialize.UpdateStatus2Init(task); err != nil {
		slog.Error("update status to init failed, will retry", "task", task, "err", err)
		time.AfterFunc(this.opt.NormalTaskHandleDelta, func() {
			if this.updateStatus2InitWithRetry(task) {
				this.Start() // 还原成功，唤醒任务循环继续处理
			}
		})
		return false
	}
	return true
}

func (this *task_mgr) sleep(index uint8) uint8 {
	loopLength := len(this.opt.OrderTaskHandleDelta)
	if loopLength == 0 {
		time.Sleep(time.Second)
		return index + 1
	}
	si := int(index) % loopLength
	sv := this.opt.OrderTaskHandleDelta[si]
	time.Sleep(sv)
	index++
	return index
}

// handdleTaskByType 有序任务串行处理：持有跨实例锁（若支持），处理完同类型所有任务后释放。
func (this *task_mgr) handdleTaskByType(task itask.ITask, t uint32) {
	slog.Info("handdleTaskByType", "type", t)
	isPanic := false
	defer func() {
		if locker, ok := this.task_serialize.(serialize.IOrderLocker); ok {
			if err := locker.UnlockOrderType(t, this.instanceID); err != nil {
				slog.Error("unlock order type failed", "type", t, "err", err)
			}
		}
		this.pop_handling_order_task_types(t)
		slog.Info("defer handdleTaskByType", "type", t)
		<-this.concurrenceNum
		this.wg.Done()
		if isPanic {
			// 当前任务被标记死信，但可能还有同类型任务未处理，唤醒 run_loop 继续
			this.Start()
		}
	}()

	if isPanic = this._handdleTaskByType(task); isPanic {
		return
	}

here:
	task, err := this.task_serialize.NextByType(t)
	if err != nil {
		if err == itask.ErrNoTask {
			return
		}
		slog.Error("next by type failed", "err", err)
		time.Sleep(time.Second)
		goto here
	}
	if isPanic = this._handdleTaskByType(task); isPanic {
		return
	}
	goto here
}

// _handdleTaskByType 处理单个有序任务：带 panic 兜底与重试，重试耗尽则标记死信。
func (this *task_mgr) _handdleTaskByType(task itask.ITask) (isPanic bool) {
	var index uint8
here:
	err := this.handleTaskSafely(task)
	if err != nil {
		index = this.sleep(index)
		if index == 255 {
			slog.Error("order task isPanic", "type", task.GetType(), "err", err)
			// 标记死信，避免任务永久卡死或阻塞同类型任务；不再自动重试
			if dl, ok := this.task_serialize.(serialize.IDeadLetter); ok {
				if err1 := dl.MarkDead(task); err1 != nil {
					slog.Error("mark dead failed", "task", task, "err", err1)
				}
			}
			isPanic = true
			return
		}
		goto here
	} else { // 处理成功，删除任务
		this.removeWithRetry(task)
	}
	return
}
