package model

import (
	"neurodb/pkg/common"
)

type LinearModel struct {
	Slope     float64
	Intercept float64
	n         float64
	sumX      float64
	sumY      float64
	sumXY     float64
	sumXX     float64
}

func NewLinearModel() *LinearModel {
	return &LinearModel{}
}

func (lm *LinearModel) Train(keys []common.KeyType) {
	lm.n = float64(len(keys))
	lm.sumX, lm.sumY, lm.sumXY, lm.sumXX = 0, 0, 0, 0

	for i, key := range keys {
		x := float64(key)
		y := float64(i)

		lm.sumX += x
		lm.sumY += y
		lm.sumXY += x * y
		lm.sumXX += x * x
	}
	lm.solve()
}

func (lm *LinearModel) TrainWithPos(keys []common.KeyType, positions []int) {
	lm.n = float64(len(keys))
	lm.sumX, lm.sumY, lm.sumXY, lm.sumXX = 0, 0, 0, 0

	for i, key := range keys {
		x := float64(key)
		y := float64(positions[i])

		lm.sumX += x
		lm.sumY += y
		lm.sumXY += x * y
		lm.sumXX += x * x
	}
	lm.solve()
}

func (lm *LinearModel) Update(key common.KeyType, pos int) {
	x := float64(key)
	y := float64(pos)

	lm.n += 1
	lm.sumX += x
	lm.sumY += y
	lm.sumXY += x * y
	lm.sumXX += x * x

	lm.solve()
}

func (lm *LinearModel) solve() {
	denominator := lm.n*lm.sumXX - lm.sumX*lm.sumX
	if denominator == 0 {
		lm.Slope = 0
		lm.Intercept = 0
	} else {
		lm.Slope = (lm.n*lm.sumXY - lm.sumX*lm.sumY) / denominator
		lm.Intercept = (lm.sumY - lm.Slope*lm.sumX) / lm.n
	}
}

func (lm *LinearModel) Predict(key common.KeyType) int {
	return int(lm.Slope*float64(key) + lm.Intercept)
}
