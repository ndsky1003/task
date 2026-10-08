package task

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/ndsky1003/task/itask"
	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/taskmgrstatus"
)

// TestAddFillsTimeFields Add 应为零值 UpdateTime/CreateTime 自动填充。
func TestAddFillsTimeFields(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_addtime", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)
	op := &fnOperator{fn: func(tk itask.ITask) error { return nil }}
	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(ser, op, opt)

	tk := &testTask{ID: primitive.NewObjectID(), Type: 4, Payload: "t"} // 时间字段留零值
	if err := mgr.Add(tk); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if is_zero_time(tk.GetUpdateTime()) {
		t.Fatal("Add 后 UpdateTime 应为非零")
	}
	if is_zero_time(tk.GetCreateTime()) {
		t.Fatal("Add 后 CreateTime 应为非零")
	}
}

// TestOrderTasksDifferentTypesParallel 不同类型的有序任务应并行执行。
func TestOrderTasksDifferentTypesParallel(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_order_parallel", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	start := make(chan uint32, 2)
	release := make(chan struct{})
	op := &fnOperator{fn: func(tk itask.ITask) error {
		start <- tk.GetType()
		<-release
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(10)
	mgr := NewTaskMgr(ser, op, opt)

	now := time.Now()
	if err := mgr.Add(&testTask{ID: primitive.NewObjectID(), Type: 11, Order: true, CreateTime: now, Payload: "p1"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := mgr.Add(&testTask{ID: primitive.NewObjectID(), Type: 12, Order: true, CreateTime: now, Payload: "p2"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// 第一个任务启动
	select {
	case <-start:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("第一个有序任务未启动")
	}
	// 第二个（不同 type）应无需等第一个完成即启动，证明并行
	select {
	case <-start:
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("不同 type 的有序任务未并行执行")
	}
	close(release)
}

// TestOrderTaskPanicOnRetryOverflow 有序任务持续失败至重试次数溢出，应放弃该类型且不删除任务。
func TestOrderTaskPanicOnRetryOverflow(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_order_panic", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	var count atomic.Int32
	op := &fnOperator{fn: func(tk itask.ITask) error {
		count.Add(1)
		return fmt.Errorf("永远失败")
	}}
	opt := Options()
	opt.SetConcurrenceNum(5)
	opt.SetOrderTaskHandleDelta([]time.Duration{time.Nanosecond})
	mgr := NewTaskMgr(ser, op, opt)

	id := primitive.NewObjectID()
	if err := mgr.Add(&testTask{ID: id, Type: 13, Order: true, CreateTime: time.Now(), Payload: "panic"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool { return count.Load() >= 255 })
	// isPanic 后任务不应被删除
	if !docExists(db, coll, id) {
		t.Fatal("重试溢出后任务不应被删除")
	}
	// 且不再继续重试
	time.Sleep(500 * time.Millisecond)
	c1 := count.Load()
	time.Sleep(500 * time.Millisecond)
	if count.Load() != c1 {
		t.Fatalf("重试溢出后不应继续重试: %d -> %d", c1, count.Load())
	}
}

// TestAddAfterStopRestarts Stop 后 Add 应能重新启动处理循环。
func TestAddAfterStopRestarts(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_restart", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	handled := make(chan struct{}, 1)
	op := &fnOperator{fn: func(tk itask.ITask) error {
		handled <- struct{}{}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(ser, op, opt)

	// 等 run_loop 空转自动停止
	waitFor(t, 5*time.Second, func() bool {
		return mgr.status.Load() == uint32(taskmgrstatus.Stop)
	})
	if !mgr.Stop() {
		t.Fatal("空闲时 Stop 应返回 true")
	}

	if err := mgr.Add(&testTask{ID: primitive.NewObjectID(), Type: 1, Payload: "after-stop"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop 后 Add 未能重新启动处理")
	}
}

// TestOrderTasksSameTypeSerial 同一类型多个有序任务应严格串行（并发峰值恒为 1）。
func TestOrderTasksSameTypeSerial(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_order_same", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	var running, peak, done atomic.Int32
	op := &fnOperator{fn: func(tk itask.ITask) error {
		c := running.Add(1)
		for {
			p := peak.Load()
			if c <= p || peak.CompareAndSwap(p, c) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		done.Add(1)
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(10)
	mgr := NewTaskMgr(ser, op, opt)

	base := time.Now()
	for i := 0; i < 5; i++ {
		if err := mgr.Add(&testTask{
			ID:         primitive.NewObjectID(),
			Type:       20,
			Order:      true,
			CreateTime: base.Add(time.Duration(i) * time.Second),
			Payload:    fmt.Sprintf("s-%d", i),
		}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	waitFor(t, 10*time.Second, func() bool { return done.Load() == 5 })

	// 同类型有序任务并发峰值必须为 1
	if p := peak.Load(); p != 1 {
		t.Fatalf("同类型有序任务应严格串行, 并发峰值 %d, 期望 1", p)
	}
}
