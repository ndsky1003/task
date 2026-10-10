package task

import (
	"reflect"
	"testing"
	"time"
)

// TestOptionClampConcurrenceNum 并发数超过 1000 应被截断。
func TestOptionClampConcurrenceNum(t *testing.T) {
	o := Options()
	o.SetConcurrenceNum(1001)
	if o.ConcurrenceNum != 1000 {
		t.Fatalf("期望并发数被截断为 1000，got %d", o.ConcurrenceNum)
	}
	o.SetConcurrenceNum(500)
	if o.ConcurrenceNum != 500 {
		t.Fatalf("期望 500，got %d", o.ConcurrenceNum)
	}
}

// TestOptionSetOrderTaskHandleDeltaEmpty 空序列不应改变重试间隔。
func TestOptionSetOrderTaskHandleDeltaEmpty(t *testing.T) {
	o := Options()
	before := append([]time.Duration(nil), o.OrderTaskHandleDelta...)
	o.SetOrderTaskHandleDelta(nil)
	if !reflect.DeepEqual(o.OrderTaskHandleDelta, before) {
		t.Fatal("空序列不应改变 OrderTaskHandleDelta")
	}
	o.SetOrderTaskHandleDelta([]time.Duration{})
	if !reflect.DeepEqual(o.OrderTaskHandleDelta, before) {
		t.Fatal("空切片不应改变 OrderTaskHandleDelta")
	}
}

// TestOptionSetOrderTaskMaxRetry 最大重试次数仅接受正值。
func TestOptionSetOrderTaskMaxRetry(t *testing.T) {
	o := Options()
	o.SetOrderTaskMaxRetry(5)
	if o.OrderTaskMaxRetry != 5 {
		t.Fatalf("期望 5，got %d", o.OrderTaskMaxRetry)
	}
	o.SetOrderTaskMaxRetry(0) // 零值不应覆盖
	if o.OrderTaskMaxRetry != 5 {
		t.Fatalf("零值不应覆盖, got %d", o.OrderTaskMaxRetry)
	}
}

// TestOptionSetOrderConcurrenceNum 有序任务并发数上限与截断。
func TestOptionSetOrderConcurrenceNum(t *testing.T) {
	o := Options()
	o.SetOrderConcurrenceNum(1001)
	if o.OrderConcurrenceNum != 1000 {
		t.Fatalf("期望截断为 1000，got %d", o.OrderConcurrenceNum)
	}
	o.SetOrderConcurrenceNum(5)
	if o.OrderConcurrenceNum != 5 {
		t.Fatalf("期望 5，got %d", o.OrderConcurrenceNum)
	}
	o.SetOrderConcurrenceNum(0) // 零值不应覆盖
	if o.OrderConcurrenceNum != 5 {
		t.Fatalf("零值不应覆盖, got %d", o.OrderConcurrenceNum)
	}
}

// TestOptionMerge 校验 Merge 只覆盖非零值。
func TestOptionMerge(t *testing.T) {
	o := Options()
	o.SetConcurrenceNum(100)

	// 只设置 NormalTaskHandleDelta 时，不应覆盖并发数
	delta := &Option{NormalTaskHandleDelta: 2 * time.Second}
	o.Merge(delta)
	if o.ConcurrenceNum != 100 {
		t.Fatalf("期望并发数保持 100，got %d", o.ConcurrenceNum)
	}
	if o.NormalTaskHandleDelta != 2*time.Second {
		t.Fatalf("期望 NormalTaskHandleDelta=2s，got %v", o.NormalTaskHandleDelta)
	}
}

// TestIsZeroTime 校验零值时间判定。
func TestIsZeroTime(t *testing.T) {
	if !is_zero_time(time.Time{}) {
		t.Fatal("零值 time.Time{} 应判定为 zero")
	}
	if !is_zero_time(time.Unix(0, 0)) {
		t.Fatal("time.Unix(0,0) 应判定为 zero")
	}
	if is_zero_time(time.Now()) {
		t.Fatal("time.Now() 不应判定为 zero")
	}
}
