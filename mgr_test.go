package task

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/task"
	"github.com/ndsky1003/task/taskmgrstatus"
	"github.com/ndsky1003/task/taskstatus"
)

// ---------- 测试基础设施 ----------

// testPayload 测试业务数据。
type testPayload struct {
	V string `json:"v"`
}

var (
	mongoClient    *mongo.Client
	mongoAvailable bool
)

func mongoURI() string {
	if u := os.Getenv("MONGO_URI"); u != "" {
		return u
	}
	return "mongodb://localhost:27017"
}

func TestMain(m *testing.M) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI()))
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = client.Ping(ctx, nil)
		cancel()
		if err == nil {
			mongoClient = client
			mongoAvailable = true
		}
	}
	code := m.Run()
	if mongoClient != nil {
		_ = mongoClient.Disconnect(context.Background())
	}
	os.Exit(code)
}

func requireMongo(t *testing.T) {
	t.Helper()
	if !mongoAvailable {
		t.Skip("MongoDB 不可用，跳过集成测试")
	}
}

func dropCollection(t *testing.T, db, coll string) {
	t.Helper()
	requireMongo(t)
	_ = mongoClient.Database(db).Collection(coll).Drop(context.Background())
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("条件在 %v 内未满足", timeout)
}

// docExists 判断指定 _id 的文档是否仍存在。
func docExists(db, coll, id string) bool {
	var d bson.M
	err := mongoClient.Database(db).Collection(coll).FindOne(context.Background(), bson.M{"_id": id}).Decode(&d)
	return err == nil
}

func assertPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("期望 panic(%q)，但没有 panic", want)
		}
		if !strings.Contains(fmt.Sprint(r), want) {
			t.Fatalf("panic 内容不匹配: got %v, want contains %q", r, want)
		}
	}()
	fn()
}

// mkPayload 生成测试业务数据字节。
func mkPayload(v string) []byte {
	return []byte(`{"v":"` + v + `"}`)
}

// payloadOf 反序列化测试业务数据，返回 v 值。
func payloadOf(t *task.Task) string {
	var p testPayload
	_ = json.Unmarshal(t.Data, &p)
	return p.V
}

// newTestTask 构造一个测试任务（用于 serialize 层直接操作）。
func newTestTask(typ uint32, order bool, v string) *task.Task {
	return &task.Task{
		ID:    primitive.NewObjectID().Hex(),
		Type:  typ,
		Order: order,
		Data:  mkPayload(v),
	}
}

// addTask 通过 mgr 添加任务并返回任务 ID。
func addTask(t *testing.T, mgr *task_mgr, typ uint32, order bool, v string) string {
	t.Helper()
	id := primitive.NewObjectID().Hex()
	if err := mgr.Add(mkPayload(v), task.Meta{ID: id, Type: typ, Order: order}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	return id
}

// ---------- 操作器 ----------

type fnOperator struct {
	fn func(*task.Task) error
}

func (o *fnOperator) HandleTask(t *task.Task) error {
	if o.fn == nil {
		return nil
	}
	return o.fn(t)
}

// ---------- 测试用例 ----------

// TestNewTaskMgrNilPanic 校验构造参数为 nil 时 panic。
func TestNewTaskMgrNilPanic(t *testing.T) {
	requireMongo(t)

	assertPanic(t, "task_serialize must not nil", func() {
		NewTaskMgr(nil, &fnOperator{})
	})

	db, coll := "task_test_nil", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)
	assertPanic(t, "op must not nil", func() {
		NewTaskMgr(ser, nil)
	})
}

// TestNormalTaskSuccessAndRemove 普通任务处理成功后应被删除。
func TestNormalTaskSuccessAndRemove(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_normal", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	handled := make(chan string, 16)
	op := &fnOperator{fn: func(tk *task.Task) error {
		handled <- tk.ID
		return nil
	}}

	opt := Options()
	opt.SetConcurrenceNum(4)
	mgr := NewTaskMgr(ser, op, opt)

	id := addTask(t, mgr, 1, false, "hello")

	select {
	case got := <-handled:
		if got != id {
			t.Fatalf("处理的任务 ID 不符: got %v, want %v", got, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("任务未被处理")
	}

	waitFor(t, 5*time.Second, func() bool { return !docExists(db, coll, id) })
}

// TestNormalTaskRetryThenSuccess 普通任务首次失败后应重试并最终成功删除。
func TestNormalTaskRetryThenSuccess(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_retry", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	var count atomic.Int32
	op := &fnOperator{fn: func(tk *task.Task) error {
		if count.Add(1) == 1 {
			return fmt.Errorf("临时失败")
		}
		return nil
	}}

	opt := Options()
	opt.SetConcurrenceNum(1)
	opt.SetNormalTaskHandleDelta(50 * time.Millisecond)
	mgr := NewTaskMgr(ser, op, opt)

	id := addTask(t, mgr, 2, false, "retry")

	waitFor(t, 8*time.Second, func() bool {
		return count.Load() >= 2 && !docExists(db, coll, id)
	})
}

// TestOrderTaskSerialExecution 同一类型的有序任务应严格按 CreateTime 顺序串行执行。
func TestOrderTaskSerialExecution(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_order", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

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
	mgr := NewTaskMgr(ser, op, opt)

	base := time.Now()
	for i, v := range []string{"a", "b", "c"} {
		id := primitive.NewObjectID().Hex()
		b := mkPayload(v)
		if err := mgr.add(&task.Task{ID: id, Type: 7, Order: true, CreateTime: base.Add(time.Duration(i) * time.Second), Data: b}); err != nil {
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

// TestConcurrentNormalTasks 多个普通任务应并发执行，且并发峰值不超过上限。
func TestConcurrentNormalTasks(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_concurrent", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	const maxWant = 5
	var cur, peak atomic.Int32
	var done atomic.Int32
	op := &fnOperator{fn: func(tk *task.Task) error {
		c := cur.Add(1)
		for {
			p := peak.Load()
			if c <= p || peak.CompareAndSwap(p, c) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		cur.Add(-1)
		done.Add(1)
		return nil
	}}

	opt := Options()
	opt.SetConcurrenceNum(maxWant)
	mgr := NewTaskMgr(ser, op, opt)

	const n = 20
	for i := 0; i < n; i++ {
		addTask(t, mgr, 3, false, fmt.Sprintf("c-%d", i))
	}

	waitFor(t, 10*time.Second, func() bool { return done.Load() == n })
	if p := peak.Load(); p > maxWant {
		t.Fatalf("并发峰值 %d 超过上限 %d", p, maxWant)
	}
	if p := peak.Load(); p < 2 {
		t.Fatalf("并发峰值 %d 过低，未体现并发能力", p)
	}
}

// TestMixedOrderAndNormal 有序任务与普通任务并存时都应被处理。
func TestMixedOrderAndNormal(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_mixed", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	var mu sync.Mutex
	handled := map[string]bool{}
	op := &fnOperator{fn: func(tk *task.Task) error {
		p := payloadOf(tk)
		mu.Lock()
		handled[p] = true
		mu.Unlock()
		return nil
	}}

	opt := Options()
	opt.SetConcurrenceNum(10)
	mgr := NewTaskMgr(ser, op, opt)

	// 一个有序任务 + 5 个普通任务
	{
		id := primitive.NewObjectID().Hex()
		b := mkPayload("ordered-1")
		if err := mgr.add(&task.Task{ID: id, Type: 1, Order: true, CreateTime: time.Now().Add(-time.Second), Data: b}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	for i := 0; i < 5; i++ {
		addTask(t, mgr, 2, false, fmt.Sprintf("normal-%d", i))
	}

	waitFor(t, 8*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(handled) == 6
	})
}

// TestInitRestoreHandlingStatus 启动时应把异常退出遗留的「处理中」任务还原并继续处理。
func TestInitRestoreHandlingStatus(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_init", "tasks"
	dropCollection(t, db, coll)

	id := primitive.NewObjectID().Hex()
	_, err := mongoClient.Database(db).Collection(coll).InsertOne(context.Background(), task.Task{
		ID:         id,
		Type:       9,
		UpdateTime: time.Now(),
		CreateTime: time.Now(),
		Status:     taskstatus.Handling,
		Data:       []byte(`{"v":"stale"}`),
	})
	if err != nil {
		t.Fatalf("插入遗留任务失败: %v", err)
	}

	handled := make(chan struct{}, 1)
	ser := serialize.Mongo(mongoClient, db, coll)
	op := &fnOperator{fn: func(tk *task.Task) error {
		if tk.ID == id {
			handled <- struct{}{}
		}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	_ = NewTaskMgr(ser, op, opt)

	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("遗留 Handling 任务未被还原并处理")
	}
}

// TestStopWhenIdle 无任务时空转的循环应自动停止，且再次 Stop 返回 true。
func TestStopWhenIdle(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_stop", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)
	op := &fnOperator{}

	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(ser, op, opt)

	waitFor(t, 5*time.Second, func() bool {
		return mgr.status.Load() == uint32(taskmgrstatus.Stop)
	})
	if !mgr.Stop() {
		t.Fatal("空闲状态 Stop() 应返回 true")
	}
}
