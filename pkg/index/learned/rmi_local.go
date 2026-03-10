package learned

import (
	"encoding/json"
	"os"
)

// RMILocalModel is inference-only: slope/intercept from Python-trained JSON; Go only runs Predict.
type RMILocalModel struct {
	GlobalMin int64     `json:"global_min"`
	GlobalMax int64     `json:"global_max"`
	Fanout    int       `json:"fanout"`
	Root      leafCoeff `json:"root"`
	Leaves    []leafCoeff `json:"leaves"`
	MinErr    int       `json:"min_err"`
	MaxErr    int       `json:"max_err"`
	NKeys     int       `json:"key_count"` // key count at train time, for CBO
}

type leafCoeff struct {
	Slope     float64 `json:"slope"`
	Intercept float64 `json:"intercept"`
}

// Predict runs inference: one multiply-add.
func (m *RMILocalModel) Predict(key int64) int {
	if m == nil || m.Fanout == 0 {
		return 0
	}
	keyRange := float64(m.GlobalMax - m.GlobalMin)
	if keyRange <= 0 {
		keyRange = 1
	}
	bucketIdx := int(float64(key-m.GlobalMin) / keyRange * float64(m.Fanout))
	if bucketIdx >= m.Fanout {
		bucketIdx = m.Fanout - 1
	}
	if bucketIdx < 0 {
		bucketIdx = 0
	}
	if bucketIdx >= len(m.Leaves) {
		return 0
	}
	leaf := &m.Leaves[bucketIdx]
	return int(leaf.Slope*float64(key) + leaf.Intercept)
}

// ErrorBound implements the CBO estimator interface.
func (m *RMILocalModel) ErrorBound() (minErr, maxErr int) {
	if m == nil {
		return 0, 0
	}
	return m.MinErr, m.MaxErr
}

// KeyCount implements the CBO estimator interface.
func (m *RMILocalModel) KeyCount() int {
	if m == nil {
		return 0
	}
	return m.NKeys
}

// LoadFromJSON loads from Python train_rmi.py output .li (JSON).
func LoadFromJSON(path string) (*RMILocalModel, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m RMILocalModel
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
