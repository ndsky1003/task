package task

// Stats 任务管理器运行统计（累计值 + 当前值）。
type Stats struct {
	Added    uint64 // 累计添加的任务数
	Handled  uint64 // 累计成功处理数
	Failed   uint64 // 累计失败次数（含重试）
	Dead     uint64 // 累计死信数
	Handling int64  // 当前处理中的任务数
}

// Stats 返回任务管理器的运行统计快照。
// 可周期性拉取并接入 Prometheus 等监控系统。
func (this *task_mgr) Stats() Stats {
	return Stats{
		Added:    this.added.Load(),
		Handled:  this.handled.Load(),
		Failed:   this.failed.Load(),
		Dead:     this.dead.Load(),
		Handling: this.handling.Load(),
	}
}
