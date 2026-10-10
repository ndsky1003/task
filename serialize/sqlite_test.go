package serialize

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/ndsky1003/task/task"
)

// ---------- 测试辅助 ----------

func newSqliteDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("打开 sqlite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func sqliteStatus(t *testing.T, db *sql.DB, table, id string) int {
	t.Helper()
	var status int
	if err := db.QueryRow("SELECT status FROM "+table+" WHERE id=?", id).Scan(&status); err != nil {
		t.Fatalf("查询 status: %v", err)
	}
	return status
}

func mkTask(id string, typ uint32, upd, create time.Time, order bool, v string) *task.Task {
	return &task.Task{
		ID:         id,
		Type:       typ,
		UpdateTime: upd,
		CreateTime: create,
		Order:      order,
		Data:       []byte(`{"v":"` + v + `"}`),
	}
}

// ---------- 构造校验 ----------

func TestSqliteNilDBPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil || !strings.Contains(r.(string), "db is nil") {
			t.Fatalf("期望 db is nil panic, got %v", r)
		}
	}()
	Sqlite(nil, "tasks")
}

func TestSqliteInvalidTablePanic(t *testing.T) {
	db := newSqliteDB(t)
	defer func() {
		if r := recover(); r == nil || !strings.Contains(r.(string), "invalid table") {
			t.Fatalf("期望 invalid table panic, got %v", r)
		}
	}()
	Sqlite(db, "bad-name; DROP TABLE x")
}

// ---------- 基本流程 ----------

func TestSqliteAddNextRemove(t *testing.T) {
	db := newSqliteDB(t)
	ser := Sqlite(db, "tasks")

	now := time.Now()
	if err := ser.Add(mkTask("task-1", 1, now.Add(-time.Hour), now, false, "hello")); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := ser.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got.ID != "task-1" {
		t.Fatalf("Next 返回错误任务: got %v", got.ID)
	}
	// 业务数据应通过 JSON 完整还原
	if string(got.Data) != `{"v":"hello"}` {
		t.Fatalf("Data 未正确还原: %s", got.Data)
	}

	if err := ser.Remove(got); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM tasks").Scan(&count); err != nil {
		t.Fatalf("查询: %v", err)
	}
	if count != 0 {
		t.Fatalf("Remove 后应无任务, got %d", count)
	}
}

func TestSqliteNextNoTask(t *testing.T) {
	db := newSqliteDB(t)
	ser := Sqlite(db, "tasks")

	if _, err := ser.Next(); !errors.Is(err, task.ErrNoTask) {
		t.Fatalf("期望 ErrNoTask, got %v", err)
	}
	if _, err := ser.NextByType(1); !errors.Is(err, task.ErrNoTask) {
		t.Fatalf("期望 ErrNoTask, got %v", err)
	}
}

func TestSqliteNextExcludeTypes(t *testing.T) {
	db := newSqliteDB(t)
	ser := Sqlite(db, "tasks")

	now := time.Now()
	_ = ser.Add(mkTask("t1", 1, now, now, false, "a"))
	_ = ser.Add(mkTask("t2", 2, now, now, false, "b"))

	got, err := ser.Next(1)
	if err != nil {
		t.Fatalf("Next(1): %v", err)
	}
	if got.ID != "t2" {
		t.Fatalf("Next(1) 应排除 type=1, got %v", got.ID)
	}
}

func TestSqliteNextByTypeOrder(t *testing.T) {
	db := newSqliteDB(t)
	ser := Sqlite(db, "tasks")

	base := time.Now()
	// 逆序插入，NextByType 按 CreateTime 升序返回
	for i := 0; i < 3; i++ {
		id := string(rune('a' + i))
		if err := ser.Add(mkTask(id, 9, base, base.Add(time.Duration(2-i)*time.Second), false, id)); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	want := []string{"c", "b", "a"}
	for i := 0; i < 3; i++ {
		got, err := ser.NextByType(9)
		if err != nil {
			t.Fatalf("第 %d 次 NextByType: %v", i, err)
		}
		if got.ID != want[i] {
			t.Fatalf("第 %d 次 NextByType 顺序错误: got %v, want %v", i, got.ID, want[i])
		}
	}
}

func TestSqliteNextUpdatesStatus(t *testing.T) {
	db := newSqliteDB(t)
	ser := Sqlite(db, "tasks")

	now := time.Now()
	if err := ser.Add(mkTask("t1", 1, now, now, false, "a")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := ser.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if s := sqliteStatus(t, db, "tasks", "t1"); s != statusHandling {
		t.Fatalf("Next 后 status 应为 Handling, got %d", s)
	}
	if _, err := ser.Next(); !errors.Is(err, task.ErrNoTask) {
		t.Fatalf("期望 ErrNoTask, got %v", err)
	}
}

func TestSqliteHasNext(t *testing.T) {
	db := newSqliteDB(t)
	ser := Sqlite(db, "tasks")

	if b, err := ser.HasNext(); err != nil || b {
		t.Fatalf("空表 HasNext 应为 false, got %v err %v", b, err)
	}
	now := time.Now()
	_ = ser.Add(mkTask("t1", 1, now, now, false, "a"))
	_ = ser.Add(mkTask("t2", 2, now, now, false, "b"))
	if b, _ := ser.HasNext(); !b {
		t.Fatal("有任务 HasNext 应为 true")
	}
	if b, _ := ser.HasNext(1); !b {
		t.Fatal("排除 type=1 后仍有 type=2, HasNext(1) 应为 true")
	}
	if b, _ := ser.HasNext(1, 2); b {
		t.Fatal("排除 1,2 后 HasNext 应为 false")
	}
}

func TestSqliteUpdateStatus2Init(t *testing.T) {
	db := newSqliteDB(t)
	ser := Sqlite(db, "tasks")

	now := time.Now()
	if err := ser.Add(mkTask("t1", 1, now, now, false, "a")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	task, _ := ser.Next()
	if err := ser.UpdateStatus2Init(task); err != nil {
		t.Fatalf("UpdateStatus2Init: %v", err)
	}
	if s := sqliteStatus(t, db, "tasks", "t1"); s != statusInit {
		t.Fatalf("还原后 status 应为 Init, got %d", s)
	}
}

func TestSqliteMarkDead(t *testing.T) {
	db := newSqliteDB(t)
	ser := Sqlite(db, "tasks")

	now := time.Now()
	if err := ser.Add(mkTask("t1", 1, now, now, false, "a")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	task, _ := ser.Next()
	if err := ser.MarkDead(task); err != nil {
		t.Fatalf("MarkDead: %v", err)
	}
	if s := sqliteStatus(t, db, "tasks", "t1"); s != statusDead {
		t.Fatalf("MarkDead 后 status 应为 Dead, got %d", s)
	}
}

func TestSqliteRecover(t *testing.T) {
	db := newSqliteDB(t)
	ser := Sqlite(db, "tasks")

	now := time.Now()
	_ = ser.Add(mkTask("t1", 1, now, now, false, "a"))
	_ = ser.Add(mkTask("t2", 2, now, now, false, "b"))
	_, _ = ser.NextByType(1) // t1 进入 Handling

	if err := ser.Recover(); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if s := sqliteStatus(t, db, "tasks", "t1"); s != statusInit {
		t.Fatalf("Recover 后 t1 应为 Init, got %d", s)
	}
}

func TestSqliteConcurrentNextNoDuplicate(t *testing.T) {
	db, err := sql.Open("sqlite", "file:sqlite_test_conc?mode=memory&cache=shared")
	if err != nil {
		t.Fatalf("打开 sqlite: %v", err)
	}
	db.SetMaxOpenConns(8)
	keep, err := db.Conn(context.Background())
	if err != nil {
		t.Fatalf("获取连接: %v", err)
	}
	defer keep.Close()
	t.Cleanup(func() { _ = db.Close() })

	ser := Sqlite(db, "tasks")
	const n = 50
	now := time.Now()
	for i := 0; i < n; i++ {
		if err := ser.Add(mkTask(string(rune('A'+i)), 10, now, now, false, "x")); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	var mu sync.Mutex
	got := map[string]bool{}
	var success atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tk, err := ser.Next(); err == nil {
				success.Add(1)
				mu.Lock()
				got[tk.ID] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if success.Load() == 0 {
		t.Fatal("并发 Next 未获取到任何任务")
	}
	if int(success.Load()) != len(got) {
		t.Fatalf("并发 Next 返回重复任务: 成功 %d 次, 唯一 %d 个", success.Load(), len(got))
	}
}
