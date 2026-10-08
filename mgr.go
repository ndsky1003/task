package task

import (
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
	handling_order_task_types []uint32 // 正在执行的OrderTask的Type
	task_serialize            serialize.ITaskSerialize
	_handle_task_operator     operator.IOperator
	done                      chan struct{}
	concurrenceNum            chan struct{} // 限流
	opt                       *Option
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

// handleStatus 处理启动/停止/错误还原，全程加锁以避免并发关闭 done。
// 返回值仅对 Stop 有意义：true 表示任务循环已停止运行。
func (this *task_mgr) handleStatus(req *handleStatusReq) bool {
	this.l.Lock()
	defer this.l.Unlock()
	switch req.status {
	case taskmgrstatus.Start, taskmgrstatus.HandleError:
		if req.status == taskmgrstatus.HandleError {
			if task, ok := req.meta.(itask.ITask); ok {
				if err := this.task_serialize.UpdateStatus2Init(task); err != nil {
					slog.Error("task:%v,err:%v\n", "task", task, "err", err)
					return false
				}
			}
		}
		if !this.status.CompareAndSwap(taskmgrstatus.Stop, taskmgrstatus.Start) {
			// if !atomic.CompareAndSwapUint32(&this.status, taskmgrstatus.Stop, taskmgrstatus.Start) {
			return false
		}
		if this.done != nil {
			close(this.done)
			this.done = nil
		}
		this.done = make(chan struct{}, 1)
		go this.run_loop(this.done)
		return false
	case taskmgrstatus.Stop:
		b, err := this.task_serialize.HasNext(this.handling_order_task_types...)
		if err != nil {
			slog.Error(err.Error())
			return false
		}
		if b {
			return false
		}
		if !this.status.CompareAndSwap(taskmgrstatus.Start, taskmgrstatus.Stop) {
			// if !atomic.CompareAndSwapUint32(&this.status, taskmgrstatus.Start, taskmgrstatus.Stop) {
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
			this.concurrenceNum <- struct{}{}
			if task.IsOrder() {
				this.push_handling_order_task_types(task.GetType())
				go this.handdleTaskByType(task)
			} else {
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
		slog.Info(err.Error())
		time.Sleep(time.Second)
	}
}

func (this *task_mgr) handdleTask(task itask.ITask) {
	defer func() {
		<-this.concurrenceNum
	}()
	err := this._handle_task_operator.HandleTask(task)
	if err != nil {
		time.AfterFunc(this.opt.NormalTaskHandleDelta, func() {
			this.handleStatus(&handleStatusReq{
				status: taskmgrstatus.HandleError,
				meta:   task,
			})
		})
	} else { // 处理成功，删除任务
		if err1 := this.task_serialize.Remove(task); err1 != nil {
			slog.Error("task:%+v,err:%v", "task", task, "err1", err1)
		}
	}
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

// 某些任务按照添加顺序执行
func (this *task_mgr) handdleTaskByType(task itask.ITask) {
	t := task.GetType()
	slog.Info("handdleTaskByType:", t)
	var isPanic bool
	defer func() {
		if !isPanic {
			// 防止下次再次进入
			this.pop_handling_order_task_types(t)
		} else {
			slog.Info("task type not handle", "type", t)
		}
		slog.Info("defer handdleTaskByType")
		<-this.concurrenceNum
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
		slog.Error(err.Error())
		time.Sleep(time.Second)
		goto here
	}
	if isPanic = this._handdleTaskByType(task); isPanic {
		return
	} else {
		goto here
	}
}

func (this *task_mgr) _handdleTaskByType(task itask.ITask) (isPanic bool) {
	var index uint8
here:
	err := this._handle_task_operator.HandleTask(task)
	if err != nil {
		index = this.sleep(index)
		if index == 255 {
			slog.Error("isPanic,t:%v,err:%v", "type", task.GetType(), "err", err)
			isPanic = true
			return
		}
		goto here
	} else { // 处理成功，删除任务
		if err1 := this.task_serialize.Remove(task); err1 != nil {
			slog.Error("task:%+v\n", "task", task)
		}
	}
	return
}
