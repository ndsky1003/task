package task

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/ndsky1003/task/itask"
	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/taskstatus"
)

// ---------- 修复 1：用户处理函数 panic 不崩溃进程 ----------

// TestHandleTaskPanicRecovered 普通任务处理函数 panic 应被捕获并转为重试，而非崩溃。
func TestHandleTaskPanicRecovered(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_panic_normal", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	var count atomic.Int32
	op := &fnOperator{fn: func(tk itask.ITask) error {
		if count.Add(1) == 1 {
			panic("业务代码 panic")
		}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	opt.SetNormalTaskHandleDelta(50 * time.Millisecond)
	mgr := NewTaskMgr(ser, op, opt)

	id := primitive.NewObjectID()
	if err := mgr.Add(&testTask{ID: id, Type: 1, Payload: "panic"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// panic 被捕获后任务重试并最终成功删除；进程不崩溃（测试能走到这里）
	waitFor(t, 8*time.Second, func() bool {
		return count.Load() >= 2 && !docExists(db, coll, id)
	})
}

// TestHandleTaskPanicInOrderTask 有序任务处理函数 panic 应被捕获并重试。
func TestHandleTaskPanicInOrderTask(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_panic_order", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	var count atomic.Int32
	op := &fnOperator{fn: func(tk itask.ITask) error {
		if count.Add(1) == 1 {
			panic("业务代码 panic")
		}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	opt.SetOrderTaskHandleDelta([]time.Duration{10 * time.Millisecond})
	mgr := NewTaskMgr(ser, op, opt)

	id := primitive.NewObjectID()
	if err := mgr.Add(&testTask{ID: id, Type: 1, Order: true, CreateTime: time.Now(), Payload: "panic-order"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	waitFor(t, 8*time.Second, func() bool {
		return count.Load() >= 2 && !docExists(db, coll, id)
	})
}

// ---------- 修复 2：Remove / UpdateStatus2Init 失败自动重试 ----------

// TestRemoveRetryEventuallyDeletes 删除失败后应自动重试直至成功。
func TestRemoveRetryEventuallyDeletes(t *testing.T) {
	f := &fakeSerialize{removeFailTimes: 2}
	op := &fnOperator{fn: func(tk itask.ITask) error { return nil }}
	opt := Options()
	opt.SetConcurrenceNum(2)
	opt.SetNormalTaskHandleDelta(20 * time.Millisecond)
	mgr := NewTaskMgr(f, op, opt)

	if err := mgr.Add(&testTask{ID: primitive.NewObjectID(), Type: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// 前 2 次删除失败，第 3 次成功
	waitFor(t, 3*time.Second, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.removeSuccess >= 1
	})
	f.mu.Lock()
	removedCalls := len(f.removed)
	f.mu.Unlock()
	if removedCalls < 3 {
		t.Fatalf("期望至少 3 次删除尝试（2 失败 + 1 成功），实际 %d", removedCalls)
	}
}

// TestUpdateStatus2InitRetrySucceeds 状态还原临时失败后应重试并最终重新处理任务。
func TestUpdateStatus2InitRetrySucceeds(t *testing.T) {
	f := &fakeSerialize{updateFailTimes: 1}
	var count atomic.Int32
	op := &fnOperator{fn: func(tk itask.ITask) error {
		count.Add(1)
		if count.Load() == 1 {
			return errors.New("handle fail")
		}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	opt.SetNormalTaskHandleDelta(20 * time.Millisecond)
	mgr := NewTaskMgr(f, op, opt)

	if err := mgr.Add(&testTask{ID: primitive.NewObjectID(), Type: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	// 第一次处理失败 + 第一次还原失败，重试后还原成功、任务重新入队并被再次处理删除
	waitFor(t, 5*time.Second, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.removeSuccess >= 1
	})
	if c := count.Load(); c < 2 {
		t.Fatalf("期望任务被重新处理（>=2 次），实际 %d", c)
	}
}

// ---------- 修复 4：有序任务重试耗尽标记死信，不阻塞同类型 ----------

// TestIsPanicMarksDead 有序任务重试耗尽后应标记为 Dead，且不阻塞同类型其他任务。
func TestIsPanicMarksDead(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_dead", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	var failCount atomic.Int32
	op := &fnOperator{fn: func(tk itask.ITask) error {
		tt := tk.(*testTask)
		if tt.Payload == "fail" {
			failCount.Add(1)
			return fmt.Errorf("永远失败")
		}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(5)
	opt.SetOrderTaskHandleDelta([]time.Duration{time.Nanosecond})
	mgr := NewTaskMgr(ser, op, opt)

	failID := primitive.NewObjectID()
	okID := primitive.NewObjectID()
	base := time.Now()
	if err := mgr.Add(&testTask{ID: failID, Type: 50, Order: true, CreateTime: base, Payload: "fail"}); err != nil {
		t.Fatalf("Add fail: %v", err)
	}
	if err := mgr.Add(&testTask{ID: okID, Type: 50, Order: true, CreateTime: base.Add(time.Second), Payload: "ok"}); err != nil {
		t.Fatalf("Add ok: %v", err)
	}

	// fail 任务重试耗尽（255 次）
	waitFor(t, 10*time.Second, func() bool { return failCount.Load() >= 255 })
	// ok 任务应被正常处理删除，说明 isPanic 未阻塞同类型
	waitFor(t, 5*time.Second, func() bool { return !docExists(db, coll, okID) })

	// fail 任务应被标记为 Dead
	var doc testTask
	if err := mongoClient.Database(db).Collection(coll).FindOne(context.Background(), bson.M{"_id": failID}).Decode(&doc); err != nil {
		t.Fatalf("查询 fail 任务: %v", err)
	}
	if doc.Status != taskstatus.Dead {
		t.Fatalf("重试耗尽后任务状态应为 Dead, got %v", doc.Status)
	}
}

// ---------- 修复 3：跨实例有序任务锁 ----------

// TestCrossInstanceOrderLock 两个实例共享存储时，同一类型有序任务应仍保持串行。
func TestCrossInstanceOrderLock(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_cross_lock", "tasks"
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
		time.Sleep(50 * time.Millisecond)
		running.Add(-1)
		done.Add(1)
		return nil
	}}

	// 两个实例共享同一存储，模拟多实例部署
	optA := Options()
	optA.SetConcurrenceNum(10)
	optB := Options()
	optB.SetConcurrenceNum(10)
	mgrA := NewTaskMgr(ser, op, optA)
	mgrB := NewTaskMgr(ser, op, optB)

	base := time.Now()
	const n = 8
	for i := 0; i < n; i++ {
		if err := ser.Add(&testTask{
			ID:         primitive.NewObjectID(),
			Type:       40,
			Order:      true,
			CreateTime: base.Add(time.Duration(i) * time.Second),
			UpdateTime: base,
		}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	waitFor(t, 15*time.Second, func() bool { return done.Load() == n })

	if p := peak.Load(); p > 1 {
		t.Fatalf("跨实例有序任务应串行, 并发峰值 %d, 期望 1", p)
	}

	mgrA.Shutdown()
	mgrB.Shutdown()
}

// ---------- 修复 6：优雅关闭 ----------

// TestShutdownGraceful Shutdown 应等待正在处理的任务完成后返回。
func TestShutdownGraceful(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_shutdown", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	started := make(chan struct{}, 1)
	var handled atomic.Bool
	op := &fnOperator{fn: func(tk itask.ITask) error {
		started <- struct{}{}
		time.Sleep(200 * time.Millisecond)
		handled.Store(true)
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(ser, op, opt)

	id := primitive.NewObjectID()
	if err := mgr.Add(&testTask{ID: id, Type: 1, Payload: "s"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	<-started // 等任务开始处理

	mgr.Shutdown() // 应等待处理完成（200ms）

	if !handled.Load() {
		t.Fatal("Shutdown 应等待正在处理的任务完成")
	}
	if docExists(db, coll, id) {
		t.Fatal("Shutdown 后任务应已处理并删除")
	}
}

// TestAddAfterShutdownReturnsError Shutdown 后 Add 应返回错误。
func TestAddAfterShutdownReturnsError(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_shutdown_add", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)
	op := &fnOperator{}
	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(ser, op, opt)

	mgr.Shutdown()

	if err := mgr.Add(&testTask{ID: primitive.NewObjectID(), Type: 1}); err == nil {
		t.Fatal("Shutdown 后 Add 应返回错误")
	}
}
