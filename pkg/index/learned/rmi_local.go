package learned

import (
	"encoding/json"
	"os"
)

// RMILocalModel is inference-only: root routes to bucket, leaf predicts local pos; Go does two multiply-adds.
// Supports per-leaf error bounds for tight fallback search.
type RMILocalModel struct {
	GlobalMin      int64       `json:"global_min"`
	GlobalMax      int64       `json:"global_max"`
	Fanout         int         `json:"fanout"`
	Root           leafCoeff   `json:"root"`
	Leaves         []leafCoeff `json:"leaves"`
	BucketStarts   []int       `json:"bucket_starts"`
	PerLeafMinErr  []int       `json:"per_leaf_min_err"`
	PerLeafMaxErr  []int       `json:"per_leaf_max_err"`
	MinErr         int         `json:"min_err"`
	MaxErr         int         `json:"max_err"`
	NKeys          int         `json:"key_count"`
}

type leafCoeff struct {
	Slope     float64 `json:"slope"`
	Intercept float64 `json:"intercept"`
}

// bucketIndex runs root model: key -> bucket index in [0, Fanout-1].
func (m *RMILocalModel) bucketIndex(key int64) int {
	if m == nil || m.Fanout == 0 {
		return 0
	}
	// Root predicts bucket index (possibly float); clamp to [0, Fanout-1]
	p := m.Root.Slope*float64(key) + m.Root.Intercept
	b := int(p)
	if b < 0 {
		b = 0
	}
	if b >= m.Fanout {
		b = m.Fanout - 1
	}
	return b
}

// Predict returns global position: root -> bucket, leaf -> local pos, then global = bucket_starts[b] + local.
func (m *RMILocalModel) Predict(key int64) int {
	pos, _, _ := m.PredictWithBounds(key)
	return pos
}

// PredictWithBounds returns (global_pos, minErr, maxErr) for tight fallback search.
// Uses per-leaf error bounds when available; otherwise global MinErr/MaxErr.
func (m *RMILocalModel) PredictWithBounds(key int64) (globalPos int, minE, maxE int) {
	if m == nil || m.Fanout == 0 {
		return 0, 0, 0
	}
	b := m.bucketIndex(key)
	if b >= len(m.Leaves) {
		return 0, m.MinErr, m.MaxErr
	}
	leaf := &m.Leaves[b]
	localPos := int(leaf.Slope*float64(key) + leaf.Intercept)
	if localPos < 0 {
		localPos = 0
	}
	globalPos = localPos
	if b < len(m.BucketStarts) {
		globalPos = m.BucketStarts[b] + localPos
	}
	minE, maxE = m.MinErr, m.MaxErr
	if b < len(m.PerLeafMinErr) && b < len(m.PerLeafMaxErr) {
		minE, maxE = m.PerLeafMinErr[b], m.PerLeafMaxErr[b]
	}
	return globalPos, minE, maxE
}

// ErrorBound implements the CBO estimator interface (global bounds).
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
