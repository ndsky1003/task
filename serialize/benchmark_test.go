package serialize

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ndsky1003/task/task"
)

var benchNow = time.Now()

func newBenchTask(i int) *task.Task {
	return &task.Task{
		ID:         fmt.Sprintf("id-%d", i),
		Type:       uint32(i % 10),
		UpdateTime: benchNow,
		CreateTime: benchNow,
		Data:       []byte(`{"v":"x"}`),
	}
}

// ---------- 后端初始化 ----------

func newSqliteFile(b *testing.B) *sql.DB {
	b.Helper()
	f, err := os.CreateTemp("", "bench_sqlite_*.db")
	if err != nil {
		b.Fatal(err)
	}
	path := f.Name()
	_ = f.Close()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { _ = db.Close(); _ = os.Remove(path) })
	return db
}

func newSqliteFileWAL(b *testing.B) *sql.DB {
	db := newSqliteFile(b)
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;"); err != nil {
		b.Fatal(err)
	}
	return db
}

func newSqliteMem(b *testing.B) *sql.DB {
	b.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		b.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func newMongoSer(b *testing.B, coll string) *mongo_serialize {
	b.Helper()
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	_ = client.Database("bench_db").Collection(coll).Drop(context.Background())
	return Mongo(client, "bench_db", coll)
}

// ---------- Add ----------

func BenchmarkSqliteMemAdd(b *testing.B) {
	ser := Sqlite(newSqliteMem(b), "tasks")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ser.Add(newBenchTask(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqliteFileAdd(b *testing.B) {
	ser := Sqlite(newSqliteFile(b), "tasks")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ser.Add(newBenchTask(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqliteFileWALAdd(b *testing.B) {
	ser := Sqlite(newSqliteFileWAL(b), "tasks")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ser.Add(newBenchTask(i)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMongoAdd(b *testing.B) {
	ser := newMongoSer(b, "tasks_add")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ser.Add(newBenchTask(i)); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------- Next（预填充后连续取） ----------

func BenchmarkSqliteMemNext(b *testing.B) {
	ser := Sqlite(newSqliteMem(b), "tasks")
	for i := 0; i < b.N; i++ {
		_ = ser.Add(newBenchTask(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ser.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqliteFileNext(b *testing.B) {
	ser := Sqlite(newSqliteFile(b), "tasks")
	for i := 0; i < b.N; i++ {
		_ = ser.Add(newBenchTask(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ser.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMongoNext(b *testing.B) {
	ser := newMongoSer(b, "tasks_next")
	for i := 0; i < b.N; i++ {
		_ = ser.Add(newBenchTask(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := ser.Next(); err != nil {
			b.Fatal(err)
		}
	}
}

// ---------- 完整生命周期（Add + Next + Remove） ----------

func BenchmarkSqliteMemLifecycle(b *testing.B) {
	ser := Sqlite(newSqliteMem(b), "tasks")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ser.Add(newBenchTask(i)); err != nil {
			b.Fatal(err)
		}
		task, err := ser.Next()
		if err != nil {
			b.Fatal(err)
		}
		if err := ser.Remove(task); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqliteFileLifecycle(b *testing.B) {
	ser := Sqlite(newSqliteFile(b), "tasks")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ser.Add(newBenchTask(i)); err != nil {
			b.Fatal(err)
		}
		task, err := ser.Next()
		if err != nil {
			b.Fatal(err)
		}
		if err := ser.Remove(task); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSqliteFileWALLifecycle(b *testing.B) {
	ser := Sqlite(newSqliteFileWAL(b), "tasks")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ser.Add(newBenchTask(i)); err != nil {
			b.Fatal(err)
		}
		task, err := ser.Next()
		if err != nil {
			b.Fatal(err)
		}
		if err := ser.Remove(task); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMongoLifecycle(b *testing.B) {
	ser := newMongoSer(b, "tasks_lifecycle")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := ser.Add(newBenchTask(i)); err != nil {
			b.Fatal(err)
		}
		task, err := ser.Next()
		if err != nil {
			b.Fatal(err)
		}
		if err := ser.Remove(task); err != nil {
			b.Fatal(err)
		}
	}
}
