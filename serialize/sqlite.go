package serialize

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ndsky1003/task/task"
	"github.com/ndsky1003/task/taskstatus"
)

const (
	statusInit     = int(taskstatus.Init)     // 待处理
	statusHandling = int(taskstatus.Handling) // 处理中
	statusDead     = int(taskstatus.Dead)     // 死信
)

// sqlite_serialize 基于 SQLite 的任务序列化器。
// 任务整体以 JSON 形式存储在 data 列；id/type/status/时间 拆分为独立列，用于查询、排序与状态机。
type sqlite_serialize struct {
	db    *sql.DB
	table string
}

// Sqlite 创建基于 SQLite 的任务序列化器。
// db 由调用方用任意 SQLite driver（推荐 modernc.org/sqlite，纯 Go 无 CGO）打开并管理生命周期。
// 构造函数会创建表与索引，失败会 panic。
func Sqlite(db *sql.DB, table string) *sqlite_serialize {
	if db == nil {
		panic("db is nil")
	}
	if !validTableName(table) {
		panic("invalid table name")
	}
	s := &sqlite_serialize{db: db, table: table}
	if err := s.init(); err != nil {
		panic(err)
	}
	return s
}

// validTableName 校验表名，仅允许字母、数字、下划线，避免 SQL 拼接注入。
func validTableName(name string) bool {
	if name == "" {
		return false
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '_':
		default:
			return false
		}
	}
	return true
}

func (this *sqlite_serialize) init() error {
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id          TEXT PRIMARY KEY,
			type        INTEGER NOT NULL,
			status      INTEGER NOT NULL DEFAULT 0,
			update_time INTEGER NOT NULL,
			create_time INTEGER NOT NULL,
			order_flag  INTEGER NOT NULL DEFAULT 0,
			data        BLOB NOT NULL
		)`, this.table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_st_type_update ON %s(status, type, update_time)`, this.table, this.table),
		fmt.Sprintf(`CREATE INDEX IF NOT EXISTS idx_%s_st_type_create ON %s(status, type, create_time)`, this.table, this.table),
	}
	for _, s := range stmts {
		if _, err := this.db.ExecContext(context.Background(), s); err != nil {
			return err
		}
	}
	return nil
}

// Recover 恢复：将异常退出时遗留的「处理中」任务还原为初始状态，避免任务丢失。
func (this *sqlite_serialize) Recover() error {
	_, err := this.db.ExecContext(context.Background(),
		fmt.Sprintf("UPDATE %s SET status=? WHERE status=?", this.table),
		statusInit, statusHandling)
	return err
}

// Add 添加任务。框架字段拆分为独立列，业务数据以 BLOB 直接存储。
func (this *sqlite_serialize) Add(task *task.Task) error {
	orderFlag := 0
	if task.Order {
		orderFlag = 1
	}
	_, err := this.db.ExecContext(context.Background(),
		fmt.Sprintf("INSERT INTO %s(id, type, status, update_time, create_time, order_flag, data) VALUES(?,?,?,?,?,?,?)", this.table),
		task.ID,
		task.Type,
		statusInit,
		task.UpdateTime.UnixNano(),
		task.CreateTime.UnixNano(),
		orderFlag,
		task.Data,
	)
	return err
}

// Next 按 UpdateTime 顺序取下一个待处理任务（可排除类型），并原子地置为处理中。
func (this *sqlite_serialize) Next(exclude ...uint32) (*task.Task, error) {
	where, args := this.initWhere(exclude)
	return this.next(where, args, "update_time")
}

// NextByType 按 CreateTime 顺序取同类型的下一个待处理任务，并原子地置为处理中。
func (this *sqlite_serialize) NextByType(t uint32) (*task.Task, error) {
	return this.next("status=? AND type=?", []any{statusInit, t}, "create_time")
}

// initWhere 构造「待处理」任务的查询条件，可选排除某些类型。
func (this *sqlite_serialize) initWhere(exclude []uint32) (string, []any) {
	if len(exclude) == 0 {
		return "status=?", []any{statusInit}
	}
	where := "status=? AND type NOT IN (" + placeholders(len(exclude)) + ")"
	args := make([]any, 0, len(exclude)+1)
	args = append(args, statusInit)
	for _, t := range exclude {
		args = append(args, t)
	}
	return where, args
}

func placeholders(n int) string {
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// next 查询一个待处理任务，组装后通过条件更新将其置为「处理中」。
// 条件更新（WHERE status=待处理）保证并发下不会重复取出同一任务。
func (this *sqlite_serialize) next(where string, args []any, sortKey string) (*task.Task, error) {
	query := fmt.Sprintf("SELECT id, type, status, update_time, create_time, order_flag, data FROM %s WHERE %s ORDER BY %s LIMIT 1", this.table, where, sortKey)
	var (
		id         string
		typ        uint32
		status     int
		updateTime int64
		createTime int64
		orderFlag  int
		data       []byte
	)
	if err := this.db.QueryRowContext(context.Background(), query, args...).Scan(&id, &typ, &status, &updateTime, &createTime, &orderFlag, &data); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, task.ErrNoTask
		}
		return nil, err
	}

	doc := &task.Task{
		ID:         id,
		Type:       typ,
		Status:     taskstatus.T(status),
		UpdateTime: time.Unix(0, updateTime),
		CreateTime: time.Unix(0, createTime),
		Order:      orderFlag != 0,
		Data:       data,
	}

	res, err := this.db.ExecContext(context.Background(),
		fmt.Sprintf("UPDATE %s SET status=?, update_time=? WHERE id=? AND status=?", this.table),
		statusHandling, time.Now().UnixNano(), id, statusInit)
	if err != nil {
		return nil, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, task.ErrNoTask
	}
	return doc, nil
}

// HasNext 判断是否还有待处理任务（可排除类型）。
func (this *sqlite_serialize) HasNext(exclude ...uint32) (bool, error) {
	where, args := this.initWhere(exclude)
	var count int
	err := this.db.QueryRowContext(context.Background(),
		fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s", this.table, where), args...).Scan(&count)
	return count > 0, err
}

// Remove 删除任务。
func (this *sqlite_serialize) Remove(task *task.Task) error {
	_, err := this.db.ExecContext(context.Background(),
		fmt.Sprintf("DELETE FROM %s WHERE id=?", this.table), task.ID)
	return err
}

// UpdateStatus2Init 任务状态还原，等待下次执行。
func (this *sqlite_serialize) UpdateStatus2Init(task *task.Task) error {
	_, err := this.db.ExecContext(context.Background(),
		fmt.Sprintf("UPDATE %s SET status=? WHERE id=?", this.table), statusInit, task.ID)
	return err
}

// MarkDead 将任务标记为死信状态。
func (this *sqlite_serialize) MarkDead(task *task.Task) error {
	_, err := this.db.ExecContext(context.Background(),
		fmt.Sprintf("UPDATE %s SET status=? WHERE id=?", this.table), statusDead, task.ID)
	return err
}
