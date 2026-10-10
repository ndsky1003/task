package task

import (
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/task"
	"github.com/ndsky1003/task/taskstatus"
)

// newSqliteMgr 创建基于内存 SQLite 的任务管理器。
func newSqliteMgr(t *testing.T, table string, op *fnOperator, opt *Option) (*task_mgr, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("打开 sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	ser := serialize.Sqlite(db, table)
	return NewTaskMgr(ser, op, opt), db
}

// TestSqliteIntegrationNormalTask 普通任务经 Sqlite 后端完整流转。
func TestSqliteIntegrationNormalTask(t *testing.T) {
	handled := make(chan string, 8)
	op := &fnOperator{fn: func(tk *task.Task) error {
		handled <- tk.ID
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(4)
	mgr, _ := newSqliteMgr(t, "tasks", op, opt)

	id := "normal-1"
	if err := mgr.Add(mkPayload("hello"), task.Meta{ID: id, Type: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	select {
	case got := <-handled:
		if got != id {
			t.Fatalf("处理 ID 不符: got %v, want %v", got, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("任务未被处理")
	}
}

// TestSqliteIntegrationOrderSerial 有序任务经 Sqlite 后端严格串行。
func TestSqliteIntegrationOrderSerial(t *testing.T) {
	var mu sync.Mutex
	var order []string
	op := &fnOperator{fn: func(tk *task.Task) error {
		p := payloadOf(tk)
		mu.Lock()
		order = append(order, p)
		mu.Unlock()
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(10)
	mgr, _ := newSqliteMgr(t, "tasks", op, opt)

	base := time.Now()
	for i, v := range []string{"a", "b", "c"} {
		if err := mgr.add(&task.Task{
			ID:         fmt.Sprintf("o-%d", i),
			Type:       7,
			Order:      true,
			CreateTime: base.Add(time.Duration(i) * time.Second),
			Data:       []byte(fmt.Sprintf(`{"v":"%s"}`, v)),
		}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	waitFor(t, 8*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 3
	})
	mu.Lock()
	defer mu.Unlock()
	want := []string{"a", "b", "c"}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("有序任务顺序错误: got %v, want %v", order, want)
		}
	}
}

// TestSqliteIntegrationDeadLetter 有序任务失败达到最大重试次数后经 Sqlite 标记死信。
func TestSqliteIntegrationDeadLetter(t *testing.T) {
	var count atomic.Int32
	op := &fnOperator{fn: func(tk *task.Task) error {
		count.Add(1)
		return fmt.Errorf("永远失败")
	}}
	opt := Options()
	opt.SetConcurrenceNum(5)
	opt.SetOrderTaskHandleDelta([]time.Duration{time.Nanosecond})
	opt.SetOrderTaskMaxRetry(3)
	mgr, db := newSqliteMgr(t, "tasks", op, opt)

	id := "dead-1"
	if err := mgr.Add(mkPayload("dead"), task.Meta{ID: id, Type: 8, Order: true}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool { return count.Load() >= 3 })

	var status int
	if err := db.QueryRow("SELECT status FROM tasks WHERE id=?", id).Scan(&status); err != nil {
		t.Fatalf("查询 status: %v", err)
	}
	if status != int(taskstatus.Dead) {
		t.Fatalf("任务状态应为 Dead, got %d", status)
	}
}

// TestSqliteIntegrationRecover 重启后 Recover 还原遗留的「处理中」任务。
func TestSqliteIntegrationRecover(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("打开 sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })

	// 第一个实例：取到任务后模拟崩溃（阻塞不删除）
	op1 := &fnOperator{fn: func(tk *task.Task) error {
		select {} // 永久阻塞，模拟处理中崩溃
	}}
	opt1 := Options()
	opt1.SetConcurrenceNum(2)
	mgr1 := NewTaskMgr(serialize.Sqlite(db, "tasks"), op1, opt1)

	id := "stale-1"
	if err := mgr1.Add(mkPayload("stale"), task.Meta{ID: id, Type: 9}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		var status int
		db.QueryRow("SELECT status FROM tasks WHERE id=?", id).Scan(&status)
		return status == int(taskstatus.Handling)
	})

	// 第二个实例：Recover 应还原并重新处理
	handled := make(chan struct{}, 1)
	op2 := &fnOperator{fn: func(tk *task.Task) error {
		if tk.ID == id {
			handled <- struct{}{}
		}
		return nil
	}}
	opt2 := Options()
	opt2.SetConcurrenceNum(2)
	_ = NewTaskMgr(serialize.Sqlite(db, "tasks"), op2, opt2)

	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("遗留 Handling 任务未被 Recover 还原并处理")
	}
}
