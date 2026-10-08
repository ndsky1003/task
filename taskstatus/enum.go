package taskstatus

type T = uint8

const (
	Init     T = iota // 初始化
	Handling          // 处理中
	Handled           // 处理完成，一般不存在，因为处理完成直接删除
	Dead              // 死信：重试耗尽后被放弃，等待人工处理
)
