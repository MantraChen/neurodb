package model

import "neurodb/pkg/common"

type LinearModel struct {
	Slope     float64
	Intercept float64
	N         float64
	SumX      float64
	SumY      float64
	SumXY     float64
	SumXX     float64
}

func NewLinearModel() *LinearModel {
	return &LinearModel{}
}

func (lm *LinearModel) Train(keys []common.KeyType) {
	lm.N = float64(len(keys))
	lm.SumX, lm.SumY, lm.SumXY, lm.SumXX = 0, 0, 0, 0

	for i, key := range keys {
		x := float64(key)
		y := float64(i)
		lm.SumX += x
		lm.SumY += y
		lm.SumXY += x * y
		lm.SumXX += x * x
	}
	lm.solve()
}

func (lm *LinearModel) TrainWithPos(keys []common.KeyType, positions []int) {
	lm.N = float64(len(keys))
	lm.SumX, lm.SumY, lm.SumXY, lm.SumXX = 0, 0, 0, 0

	for i, key := range keys {
		x := float64(key)
		y := float64(positions[i])
		lm.SumX += x
		lm.SumY += y
		lm.SumXY += x * y
		lm.SumXX += x * x
	}
	lm.solve()
}

func (lm *LinearModel) Update(key common.KeyType, pos int) {
	x := float64(key)
	y := float64(pos)

	lm.N += 1
	lm.SumX += x
	lm.SumY += y
	lm.SumXY += x * y
	lm.SumXX += x * x
	lm.solve()
}

func (lm *LinearModel) solve() {
	denominator := lm.N*lm.SumXX - lm.SumX*lm.SumX
	if denominator == 0 {
		lm.Slope = 0
		lm.Intercept = 0
	} else {
		lm.Slope = (lm.N*lm.SumXY - lm.SumX*lm.SumY) / denominator
		lm.Intercept = (lm.SumY - lm.Slope*lm.SumX) / lm.N
	}
}

func (lm *LinearModel) Predict(key common.KeyType) int {
	return int(lm.Slope*float64(key) + lm.Intercept)
}
