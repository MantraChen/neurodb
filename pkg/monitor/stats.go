package monitor

import (
	"sync/atomic"
)

// WorkloadStats 用于统计数据库的运行时指标
type WorkloadStats struct {
	ReadCount  uint64 // 总查询次数
	WriteCount uint64 // 总写入次数
	HitCount   uint64 // 缓存/索引命中次数
}

// NewWorkloadStats 初始化
func NewWorkloadStats() *WorkloadStats {
	return &WorkloadStats{}
}

// RecordRead 记录一次查询
func (ws *WorkloadStats) RecordRead() {
	atomic.AddUint64(&ws.ReadCount, 1)
}

// RecordWrite 记录一次写入
func (ws *WorkloadStats) RecordWrite() {
	atomic.AddUint64(&ws.WriteCount, 1)
}

// RecordHit 记录一次索引命中
func (ws *WorkloadStats) RecordHit() {
	atomic.AddUint64(&ws.HitCount, 1)
}

// GetReadWriteRatio 计算读写比
// > 1.0 表示读多写少 (适合 Learned Index)
// < 1.0 表示写多读少 (适合 B-Tree)
func (ws *WorkloadStats) GetReadWriteRatio() float64 {
	reads := atomic.LoadUint64(&ws.ReadCount)
	writes := atomic.LoadUint64(&ws.WriteCount)

	if writes == 0 {
		if reads > 0 {
			return 100.0 // 无穷大，纯读负载
		}
		return 0.0
	}
	return float64(reads) / float64(writes)
}
