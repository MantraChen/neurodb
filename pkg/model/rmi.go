package model

import (
	"neurodb/pkg/common"
)

type RMIModel struct {
	globalMin common.KeyType
	globalMax common.KeyType
	fanout    int
	buckets   []*LinearModel
}

func NewRMIModel(fanout int) *RMIModel {
	return &RMIModel{
		fanout:  fanout,
		buckets: make([]*LinearModel, fanout),
	}
}

func (rmi *RMIModel) Train(keys []common.KeyType) {
	if len(keys) == 0 {
		return
	}

	rmi.globalMin = keys[0]
	rmi.globalMax = keys[len(keys)-1]

	keyRange := float64(rmi.globalMax - rmi.globalMin)
	if keyRange == 0 {
		keyRange = 1
	}

	segments := make([][]common.KeyType, rmi.fanout)
	for i := 0; i < rmi.fanout; i++ {
		segments[i] = make([]common.KeyType, 0)
	}

	bucketKeys := make([][]common.KeyType, rmi.fanout)
	bucketPoss := make([][]int, rmi.fanout)

	for i, key := range keys {
		bucketIdx := int(float64(key-rmi.globalMin) / keyRange * float64(rmi.fanout))
		if bucketIdx >= rmi.fanout {
			bucketIdx = rmi.fanout - 1
		}
		if bucketIdx < 0 {
			bucketIdx = 0
		}

		bucketKeys[bucketIdx] = append(bucketKeys[bucketIdx], key)
		bucketPoss[bucketIdx] = append(bucketPoss[bucketIdx], i)
	}

	for i := 0; i < rmi.fanout; i++ {
		rmi.buckets[i] = NewLinearModel()
		rmi.buckets[i].TrainWithPos(bucketKeys[i], bucketPoss[i])
	}
}

func (rmi *RMIModel) Predict(key common.KeyType) int {
	keyRange := float64(rmi.globalMax - rmi.globalMin)
	if keyRange == 0 {
		return 0
	}

	bucketIdx := int(float64(key-rmi.globalMin) / keyRange * float64(rmi.fanout))
	if bucketIdx >= rmi.fanout {
		bucketIdx = rmi.fanout - 1
	}
	if bucketIdx < 0 {
		bucketIdx = 0
	}

	return rmi.buckets[bucketIdx].Predict(key)
}

func (rmi *RMIModel) Update(key common.KeyType, pos int) {
	keyRange := float64(rmi.globalMax - rmi.globalMin)
	if keyRange == 0 {
		keyRange = 1
	}

	bucketIdx := int(float64(key-rmi.globalMin) / keyRange * float64(rmi.fanout))
	if bucketIdx >= rmi.fanout {
		bucketIdx = rmi.fanout - 1
	}
	if bucketIdx < 0 {
		bucketIdx = 0
	}

	rmi.buckets[bucketIdx].Update(key, pos)
}
