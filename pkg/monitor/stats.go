package monitor

import (
	"sync/atomic"
)

type WorkloadStats struct {
	ReadCount  uint64
	WriteCount uint64
	HitCount   uint64
}

func NewWorkloadStats() *WorkloadStats {
	return &WorkloadStats{}
}

func (ws *WorkloadStats) RecordRead() {
	atomic.AddUint64(&ws.ReadCount, 1)
}

func (ws *WorkloadStats) RecordWrite() {
	atomic.AddUint64(&ws.WriteCount, 1)
}

func (ws *WorkloadStats) RecordHit() {
	atomic.AddUint64(&ws.HitCount, 1)
}

func (ws *WorkloadStats) GetReadWriteRatio() float64 {
	reads := atomic.LoadUint64(&ws.ReadCount)
	writes := atomic.LoadUint64(&ws.WriteCount)

	if writes == 0 {
		if reads > 0 {
			return 100.0
		}
		return 0.0
	}
	return float64(reads) / float64(writes)
}
