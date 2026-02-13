package learned

import (
	"math/rand"
	"neurodb/pkg/common"
	"neurodb/pkg/model"
	"sort"
	"time"
)

type DiagnosticPoint struct {
	Key          int64
	RealPos      int
	PredictedPos int
	Error        int
}

type LearnedIndex struct {
	records []common.Record
	model   *model.RMIModel
	minErr  int
	maxErr  int
}

func Build(data []common.Record) *LearnedIndex {
	sort.Slice(data, func(i, j int) bool {
		return data[i].Key < data[j].Key
	})

	keys := make([]common.KeyType, len(data))
	for i, r := range data {
		keys[i] = r.Key
	}

	rmi := model.NewRMIModel(1000)
	rmi.Train(keys)

	minErr, maxErr := 0, 0
	for i, key := range keys {
		predictedPos := rmi.Predict(key)
		actualPos := i
		err := actualPos - predictedPos

		if err < minErr {
			minErr = err
		}
		if err > maxErr {
			maxErr = err
		}
	}

	return &LearnedIndex{
		records: data,
		model:   rmi,
		minErr:  minErr,
		maxErr:  maxErr,
	}
}

func (li *LearnedIndex) GetAllRecords() []common.Record {
	return li.records
}

func (li *LearnedIndex) Get(key common.KeyType) (common.ValueType, bool) {
	if len(li.records) == 0 {
		return nil, false
	}

	predictedPos := li.model.Predict(key)

	low := predictedPos + li.minErr
	high := predictedPos + li.maxErr

	if low < 0 {
		low = 0
	}
	if high >= len(li.records) {
		high = len(li.records) - 1
	}
	if low > high {
		return nil, false
	}

	if high-low < 16 {
		// Linear Scan (SIMD friendly)
		for i := low; i <= high; i++ {
			if li.records[i].Key == key {
				return li.records[i].Value, true
			}
			if li.records[i].Key > key {
				return nil, false
			}
		}
		return nil, false
	}

	slice := li.records[low : high+1]
	idx := sort.Search(len(slice), func(i int) bool {
		return slice[i].Key >= key
	})

	if idx < len(slice) && slice[idx].Key == key {
		return slice[idx].Value, true
	}
	return nil, false
}

func (li *LearnedIndex) Size() int {
	return len(li.records)
}

func (li *LearnedIndex) ExportDiagnostics() []DiagnosticPoint {
	results := make([]DiagnosticPoint, len(li.records))

	for i, record := range li.records {
		pred := li.model.Predict(record.Key)
		err := i - pred

		results[i] = DiagnosticPoint{
			Key:          int64(record.Key),
			RealPos:      i,
			PredictedPos: pred,
			Error:        err,
		}
	}
	return results
}

func (li *LearnedIndex) BenchmarkInternal(iterations int) (float64, float64, error) {
	if len(li.records) == 0 {
		return 0, 0, nil
	}

	keys := make([]common.KeyType, iterations)
	for i := 0; i < iterations; i++ {
		idx := rand.Intn(len(li.records))
		keys[i] = li.records[idx].Key
	}

	startBin := time.Now()
	for _, key := range keys {
		sort.Search(len(li.records), func(i int) bool {
			return li.records[i].Key >= key
		})
	}
	avgBin := float64(time.Since(startBin).Nanoseconds()) / float64(iterations)

	startRMI := time.Now()
	for _, key := range keys {
		pred := li.model.Predict(key)
		l, h := pred+li.minErr, pred+li.maxErr
		if l < 0 {
			l = 0
		}
		if h >= len(li.records) {
			h = len(li.records) - 1
		}

		if h-l < 16 {
			found := false
			for i := l; i <= h; i++ {
				if li.records[i].Key == key {
					found = true
					break
				}
			}
			_ = found
		} else {
			slice := li.records[l : h+1]
			sort.Search(len(slice), func(i int) bool {
				return slice[i].Key >= key
			})
		}
	}
	avgRMI := float64(time.Since(startRMI).Nanoseconds()) / float64(iterations)

	return avgBin, avgRMI, nil
}
