package model

import (
	"neurodb/pkg/common"
)

type LinearModel struct {
	Slope     float64
	Intercept float64
}

func NewLinearModel() *LinearModel {
	return &LinearModel{}
}

func (lm *LinearModel) Train(keys []common.KeyType) {
	if len(keys) == 0 {
		return
	}

	n := float64(len(keys))
	var sumX, sumY, sumXY, sumXX float64

	for i, key := range keys {
		x := float64(key)
		y := float64(i)

		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}

	denominator := n*sumXX - sumX*sumX
	if denominator == 0 {
		lm.Slope = 0
		lm.Intercept = 0
	} else {
		lm.Slope = (n*sumXY - sumX*sumY) / denominator
		lm.Intercept = (sumY - lm.Slope*sumX) / n
	}
}

func (lm *LinearModel) Predict(key common.KeyType) int {
	return int(lm.Slope*float64(key) + lm.Intercept)
}

func (lm *LinearModel) TrainWithPos(keys []common.KeyType, positions []int) {
	if len(keys) == 0 {
		return
	}

	n := float64(len(keys))
	var sumX, sumY, sumXY, sumXX float64

	for i, key := range keys {
		x := float64(key)
		y := float64(positions[i])

		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}

	denominator := n*sumXX - sumX*sumX
	if denominator == 0 {
		lm.Slope = 0
		lm.Intercept = 0
	} else {
		lm.Slope = (n*sumXY - sumX*sumY) / denominator
		lm.Intercept = (sumY - lm.Slope*sumX) / n
	}
}
