package learned

import (
	"encoding/gob"
	"math/rand"
	"neurodb/pkg/common"
	"neurodb/pkg/model"
	"os"
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
	Records []common.Record // 原始数据
	Model   *model.RMIModel
	MinErr  int
	MaxErr  int
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
		Records: data,
		Model:   rmi,
		MinErr:  minErr,
		MaxErr:  maxErr,
	}
}

func (li *LearnedIndex) Append(newData []common.Record) {
	if len(newData) == 0 {
		return
	}

	startPos := len(li.Records)
	li.Records = append(li.Records, newData...)

	for i, rec := range newData {
		globalPos := startPos + i
		li.Model.Update(rec.Key, globalPos)
	}

	for i, rec := range newData {
		globalPos := startPos + i
		predPos := li.Model.Predict(rec.Key)
		err := globalPos - predPos

		if err < li.MinErr {
			li.MinErr = err
		}
		if err > li.MaxErr {
			li.MaxErr = err
		}
	}
}

func (li *LearnedIndex) GetAllRecords() []common.Record {
	return li.Records
}

func (li *LearnedIndex) Get(key common.KeyType) (common.ValueType, bool) {
	if len(li.Records) == 0 {
		return nil, false
	}

	predictedPos := li.Model.Predict(key)

	low := predictedPos + li.MinErr
	high := predictedPos + li.MaxErr

	if low < 0 {
		low = 0
	}
	if high >= len(li.Records) {
		high = len(li.Records) - 1
	}
	if low > high {
		return nil, false
	}

	if high-low < 16 {
		for i := low; i <= high; i++ {
			if li.Records[i].Key == key {
				return li.Records[i].Value, true
			}
			if li.Records[i].Key > key {
				return nil, false
			}
		}
		return nil, false
	}

	slice := li.Records[low : high+1]
	idx := sort.Search(len(slice), func(i int) bool {
		return slice[i].Key >= key
	})

	if idx < len(slice) && slice[idx].Key == key {
		return slice[idx].Value, true
	}
	return nil, false
}

func (li *LearnedIndex) Size() int {
	return len(li.Records)
}

func (li *LearnedIndex) ExportDiagnostics() []DiagnosticPoint {
	// 采样导出，避免数据量过大
	step := 1
	if len(li.Records) > 5000 {
		step = len(li.Records) / 5000
	}

	results := make([]DiagnosticPoint, 0, len(li.Records)/step)

	for i := 0; i < len(li.Records); i += step {
		record := li.Records[i]
		pred := li.Model.Predict(record.Key)
		err := i - pred

		results = append(results, DiagnosticPoint{
			Key:          int64(record.Key),
			RealPos:      i,
			PredictedPos: pred,
			Error:        err,
		})
	}
	return results
}

func (li *LearnedIndex) BenchmarkInternal(iterations int) (float64, float64, error) {
	if len(li.Records) == 0 {
		return 0, 0, nil
	}

	keys := make([]common.KeyType, iterations)
	for i := 0; i < iterations; i++ {
		idx := rand.Intn(len(li.Records))
		keys[i] = li.Records[idx].Key
	}

	// B-Tree (Binary Search) Benchmark
	startBin := time.Now()
	for _, key := range keys {
		sort.Search(len(li.Records), func(i int) bool {
			return li.Records[i].Key >= key
		})
	}
	avgBin := float64(time.Since(startBin).Nanoseconds()) / float64(iterations)

	// Learned Index Benchmark
	startRMI := time.Now()
	for _, key := range keys {
		pred := li.Model.Predict(key)
		l, h := pred+li.MinErr, pred+li.MaxErr
		if l < 0 {
			l = 0
		}
		if h >= len(li.Records) {
			h = len(li.Records) - 1
		}

		if h-l < 16 {
			for i := l; i <= h; i++ {
				if li.Records[i].Key == key {
					break
				}
			}
		} else {
			slice := li.Records[l : h+1]
			sort.Search(len(slice), func(i int) bool {
				return slice[i].Key >= key
			})
		}
	}
	avgRMI := float64(time.Since(startRMI).Nanoseconds()) / float64(iterations)

	return avgBin, avgRMI, nil
}

func (li *LearnedIndex) Scan(lowKey, highKey common.KeyType) []common.Record {
	var res []common.Record
	if len(li.Records) == 0 {
		return res
	}

	pos := li.Model.Predict(lowKey)
	startIdx := pos + li.MinErr

	// Boundary checks
	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx >= len(li.Records) {
		startIdx = len(li.Records) - 1
	}

	// Correction scan
	for startIdx > 0 && li.Records[startIdx].Key >= lowKey {
		startIdx--
	}
	for startIdx < len(li.Records) && li.Records[startIdx].Key < lowKey {
		startIdx++
	}

	for i := startIdx; i < len(li.Records); i++ {
		rec := li.Records[i]
		if rec.Key > highKey {
			break
		}
		if rec.Key >= lowKey {
			res = append(res, rec)
		}
	}
	return res
}

func (li *LearnedIndex) Save(filename string) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := gob.NewEncoder(f)
	return enc.Encode(li)
}

func Load(filename string) (*LearnedIndex, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var li LearnedIndex
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&li); err != nil {
		return nil, err
	}
	return &li, nil
}
