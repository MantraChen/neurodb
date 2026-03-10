package learned

import (
	"encoding/binary"
	"math"
	"neurodb/pkg/common"
	"neurodb/pkg/model"
	"sort"
)

// RMISnapshot is an RMI trained on key sequence only, for SSTable footer or CBO cost estimation.
// Does not hold raw Records; serializable to file.
type RMISnapshot struct {
	Model  *model.RMIModel
	MinErr int
	MaxErr int
	Keys   []common.KeyType // optional, for ErrorBound; may omit when serializing to save space
}

// BuildFromKeys trains RMI on sorted keys and computes error bound.
func BuildFromKeys(keys []common.KeyType) *RMISnapshot {
	if len(keys) == 0 {
		return &RMISnapshot{Model: model.NewRMIModel(1000), MinErr: 0, MaxErr: 0}
	}
	sorted := make([]common.KeyType, len(keys))
	copy(sorted, keys)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	rmi := model.NewRMIModel(1000)
	rmi.Train(sorted)

	minErr, maxErr := 0, 0
	for i, key := range sorted {
		pred := rmi.Predict(key)
		err := i - pred
		if err < minErr {
			minErr = err
		}
		if err > maxErr {
			maxErr = err
		}
	}
	return &RMISnapshot{Model: rmi, MinErr: minErr, MaxErr: maxErr, Keys: sorted}
}

// Predict returns approximate position of key in sorted sequence.
func (s *RMISnapshot) Predict(key common.KeyType) int {
	if s.Model == nil {
		return 0
	}
	return s.Model.Predict(key)
}

// ErrorBound returns prediction error range [minErr, maxErr].
func (s *RMISnapshot) ErrorBound() (minErr, maxErr int) {
	return s.MinErr, s.MaxErr
}

// KeyCount returns number of keys used in training (for CBO).
func (s *RMISnapshot) KeyCount() int {
	if s.Keys != nil {
		return len(s.Keys)
	}
	return 0
}

// MarshalBinary serializes to bytes (for writing to SSTable footer).
func (s *RMISnapshot) MarshalBinary() ([]byte, error) {
	if s.Model == nil {
		return []byte{}, nil
	}
	// Simplified: save GlobalMin, GlobalMax, Fanout, per-bucket Slope/Intercept, MinErr, MaxErr
	buf := make([]byte, 0, 256)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(s.Model.GlobalMin))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(s.Model.GlobalMax))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(s.Model.Fanout))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(s.MinErr))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(s.MaxErr))
	for _, b := range s.Model.Buckets {
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(b.Slope))
		buf = binary.LittleEndian.AppendUint64(buf, math.Float64bits(b.Intercept))
	}
	return buf, nil
}

// UnmarshalBinary deserializes from bytes (read from SSTable footer).
func (s *RMISnapshot) UnmarshalBinary(data []byte) error {
	if len(data) < 8*2+4*3 {
		return nil
	}
	s.Model = &model.RMIModel{}
	s.Model.GlobalMin = common.KeyType(binary.LittleEndian.Uint64(data[0:8]))
	s.Model.GlobalMax = common.KeyType(binary.LittleEndian.Uint64(data[8:16]))
	s.Model.Fanout = int(binary.LittleEndian.Uint32(data[16:20]))
	s.MinErr = int(int32(binary.LittleEndian.Uint32(data[20:24])))
	s.MaxErr = int(int32(binary.LittleEndian.Uint32(data[24:28])))
	// LinearModel Slope/Intercept are float64; stored as bits for correct precision
	n := (len(data) - 28) / 16
	if n > s.Model.Fanout {
		n = s.Model.Fanout
	}
	s.Model.Buckets = make([]model.LinearModel, s.Model.Fanout)
	for i := 0; i < n; i++ {
		off := 28 + i*16
		s.Model.Buckets[i].Slope = math.Float64frombits(binary.LittleEndian.Uint64(data[off : off+8]))
		s.Model.Buckets[i].Intercept = math.Float64frombits(binary.LittleEndian.Uint64(data[off+8 : off+16]))
	}
	return nil
}
