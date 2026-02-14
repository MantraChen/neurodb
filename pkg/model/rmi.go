package model

import (
	"neurodb/pkg/common"
)

type RMIModel struct {
	GlobalMin common.KeyType
	GlobalMax common.KeyType
	Fanout    int
	Buckets   []LinearModel
}

func NewRMIModel(fanout int) *RMIModel {
	return &RMIModel{
		Fanout:  fanout,
		Buckets: make([]LinearModel, fanout),
	}
}

func (rmi *RMIModel) Train(keys []common.KeyType) {
	if len(keys) == 0 {
		return
	}

	rmi.GlobalMin = keys[0]
	rmi.GlobalMax = keys[len(keys)-1]

	keyRange := float64(rmi.GlobalMax - rmi.GlobalMin)
	if keyRange == 0 {
		keyRange = 1
	}

	bucketKeys := make([][]common.KeyType, rmi.Fanout)
	bucketPoss := make([][]int, rmi.Fanout)

	for i, key := range keys {
		bucketIdx := int(float64(key-rmi.GlobalMin) / keyRange * float64(rmi.Fanout))
		if bucketIdx >= rmi.Fanout {
			bucketIdx = rmi.Fanout - 1
		}
		if bucketIdx < 0 {
			bucketIdx = 0
		}

		bucketKeys[bucketIdx] = append(bucketKeys[bucketIdx], key)
		bucketPoss[bucketIdx] = append(bucketPoss[bucketIdx], i)
	}

	for i := 0; i < rmi.Fanout; i++ {
		(&rmi.Buckets[i]).TrainWithPos(bucketKeys[i], bucketPoss[i])
	}
}

func (rmi *RMIModel) Predict(key common.KeyType) int {
	keyRange := float64(rmi.GlobalMax - rmi.GlobalMin)
	if keyRange == 0 {
		return 0
	}

	bucketIdx := int(float64(key-rmi.GlobalMin) / keyRange * float64(rmi.Fanout))
	if bucketIdx >= rmi.Fanout {
		bucketIdx = rmi.Fanout - 1
	}
	if bucketIdx < 0 {
		bucketIdx = 0
	}

	return rmi.Buckets[bucketIdx].Predict(key)
}

func (rmi *RMIModel) Update(key common.KeyType, pos int) {
	keyRange := float64(rmi.GlobalMax - rmi.GlobalMin)
	if keyRange == 0 {
		keyRange = 1
	}

	bucketIdx := int(float64(key-rmi.GlobalMin) / keyRange * float64(rmi.Fanout))
	if bucketIdx >= rmi.Fanout {
		bucketIdx = rmi.Fanout - 1
	}
	if bucketIdx < 0 {
		bucketIdx = 0
	}

	(&rmi.Buckets[bucketIdx]).Update(key, pos)
}
