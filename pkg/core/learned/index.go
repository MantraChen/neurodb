package learned

import (
	"encoding/gob"
	"math/rand"
	"neurodb/pkg/common"
	"neurodb/pkg/model"
	indexlearned "neurodb/pkg/index/learned"
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

// LearnedIndexInterface is implemented by both Go-trained and Python-backed indexes for Get/Scan/heatmap.
type LearnedIndexInterface interface {
	Get(key common.KeyType) (common.ValueType, bool)
	Scan(lowKey, highKey common.KeyType) []common.Record
	ExportDiagnostics() []DiagnosticPoint
	Predict(key int64) int
	ErrorBound() (minErr, maxErr int)
	KeyCount() int
	GetAllRecords() []common.Record
}

// Ensure *LearnedIndex implements LearnedIndexInterface.
var _ LearnedIndexInterface = (*LearnedIndex)(nil)

type LearnedIndex struct {
	Records []common.Record // raw data
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

// Predict returns approximate position of key in sorted sequence (implements RMIEstimator).
func (li *LearnedIndex) Predict(key int64) int {
	if li.Model == nil {
		return 0
	}
	return li.Model.Predict(common.KeyType(key))
}

// ErrorBound returns prediction error range (implements RMIEstimator).
func (li *LearnedIndex) ErrorBound() (minErr, maxErr int) {
	return li.MinErr, li.MaxErr
}

// KeyCount returns record count (implements RMIEstimator).
func (li *LearnedIndex) KeyCount() int {
	return len(li.Records)
}

// PredictWithBounds returns (position, minErr, maxErr) for tight fallback search (per-leaf when available).
func (li *LearnedIndex) PredictWithBounds(key common.KeyType) (pos int, minE, maxE int) {
	if li.Model == nil {
		return 0, li.MinErr, li.MaxErr
	}
	return li.Model.Predict(key), li.MinErr, li.MaxErr
}

func (li *LearnedIndex) Get(key common.KeyType) (common.ValueType, bool) {
	if len(li.Records) == 0 {
		return nil, false
	}
	// Error-bounds truncation: restrict to [pos + minE, pos + maxE] only (no global fallback search).
	predictedPos, minE, maxE := li.PredictWithBounds(key)
	low := predictedPos + minE
	high := predictedPos + maxE

	if low < 0 {
		low = 0
	}
	if high >= len(li.Records) {
		high = len(li.Records) - 1
	}
	if low > high {
		return nil, false
	}

	// Tight range: linear scan when very small, else binary search
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
	// Sample export to avoid huge output
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

// PythonBackedIndex uses a Python-trained RMI (root->bucket, leaves->local pos) and merged records for Get/Scan/heatmap.
var _ LearnedIndexInterface = (*PythonBackedIndex)(nil)

type PythonBackedIndex struct {
	Model   *indexlearned.RMILocalModel
	Records []common.Record
}

// NewPythonBackedIndex builds an index that uses the Python model and per-leaf error bounds for tight fallback.
func NewPythonBackedIndex(model *indexlearned.RMILocalModel, records []common.Record) *PythonBackedIndex {
	return &PythonBackedIndex{Model: model, Records: records}
}

func (p *PythonBackedIndex) Get(key common.KeyType) (common.ValueType, bool) {
	if p.Model == nil || len(p.Records) == 0 {
		return nil, false
	}
	// Error-bounds truncation: binary search only in [pos + minE, pos + maxE] (per-leaf bounds from Python).
	pos, minE, maxE := p.Model.PredictWithBounds(int64(key))
	low := pos + minE
	high := pos + maxE
	if low < 0 {
		low = 0
	}
	if high >= len(p.Records) {
		high = len(p.Records) - 1
	}
	if low > high {
		return nil, false
	}
	if high-low < 16 {
		for i := low; i <= high; i++ {
			if p.Records[i].Key == key {
				return p.Records[i].Value, true
			}
			if p.Records[i].Key > key {
				return nil, false
			}
		}
		return nil, false
	}
	slice := p.Records[low : high+1]
	idx := sort.Search(len(slice), func(i int) bool { return slice[i].Key >= key })
	if idx < len(slice) && slice[idx].Key == key {
		return slice[idx].Value, true
	}
	return nil, false
}

func (p *PythonBackedIndex) Scan(lowKey, highKey common.KeyType) []common.Record {
	var res []common.Record
	if len(p.Records) == 0 {
		return res
	}
	// Error-bounds truncation: start scan from pos + minE (no global fallback).
	pos, minE, _ := p.Model.PredictWithBounds(int64(lowKey))
	startIdx := pos + minE
	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx >= len(p.Records) {
		startIdx = len(p.Records) - 1
	}
	for startIdx > 0 && p.Records[startIdx].Key >= lowKey {
		startIdx--
	}
	for startIdx < len(p.Records) && p.Records[startIdx].Key < lowKey {
		startIdx++
	}
	for i := startIdx; i < len(p.Records); i++ {
		rec := p.Records[i]
		if rec.Key > highKey {
			break
		}
		if rec.Key >= lowKey {
			res = append(res, rec)
		}
	}
	return res
}

func (p *PythonBackedIndex) ExportDiagnostics() []DiagnosticPoint {
	if p.Model == nil || len(p.Records) == 0 {
		return nil
	}
	step := 1
	if len(p.Records) > 5000 {
		step = len(p.Records) / 5000
	}
	results := make([]DiagnosticPoint, 0, len(p.Records)/step)
	for i := 0; i < len(p.Records); i += step {
		rec := p.Records[i]
		pred, _, _ := p.Model.PredictWithBounds(int64(rec.Key))
		results = append(results, DiagnosticPoint{
			Key:          int64(rec.Key),
			RealPos:      i,
			PredictedPos: pred,
			Error:        i - pred,
		})
	}
	return results
}

func (p *PythonBackedIndex) Predict(key int64) int {
	if p.Model == nil {
		return 0
	}
	return p.Model.Predict(key)
}

func (p *PythonBackedIndex) ErrorBound() (minErr, maxErr int) {
	if p.Model == nil {
		return 0, 0
	}
	return p.Model.ErrorBound()
}

func (p *PythonBackedIndex) KeyCount() int {
	return len(p.Records)
}

func (p *PythonBackedIndex) GetAllRecords() []common.Record {
	return p.Records
}

// BenchmarkInternal runs B-Tree vs RMI (PredictWithBounds + binary search) for dashboard; same contract as LearnedIndex.
func (p *PythonBackedIndex) BenchmarkInternal(iterations int) (float64, float64, error) {
	if p.Model == nil || len(p.Records) == 0 {
		return 0, 0, nil
	}
	keys := make([]common.KeyType, iterations)
	for i := 0; i < iterations; i++ {
		idx := rand.Intn(len(p.Records))
		keys[i] = p.Records[idx].Key
	}
	// B-Tree (binary search over full array)
	startBin := time.Now()
	for _, key := range keys {
		sort.Search(len(p.Records), func(i int) bool {
			return p.Records[i].Key >= key
		})
	}
	avgBin := float64(time.Since(startBin).Nanoseconds()) / float64(iterations)
	// RMI: PredictWithBounds + search in [low, high]
	startRMI := time.Now()
	for _, key := range keys {
		pos, minE, maxE := p.Model.PredictWithBounds(int64(key))
		low, high := pos+minE, pos+maxE
		if low < 0 {
			low = 0
		}
		if high >= len(p.Records) {
			high = len(p.Records) - 1
		}
		if low > high {
			continue
		}
		if high-low < 16 {
			for i := low; i <= high; i++ {
				if p.Records[i].Key >= key {
					break
				}
			}
		} else {
			slice := p.Records[low : high+1]
			sort.Search(len(slice), func(i int) bool { return slice[i].Key >= key })
		}
	}
	avgRMI := float64(time.Since(startRMI).Nanoseconds()) / float64(iterations)
	return avgBin, avgRMI, nil
}
