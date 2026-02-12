package model

import (
	"neurodb/pkg/common"
)

// LinearModel 简单的线性回归模型
// Pos = Slope * Key + Intercept
type LinearModel struct {
	Slope     float64
	Intercept float64
}

// NewLinearModel 创建模型
func NewLinearModel() *LinearModel {
	return &LinearModel{}
}

// Train 训练模型 (使用最小二乘法，Least Squares)
func (lm *LinearModel) Train(keys []common.KeyType) {
	if len(keys) == 0 {
		return
	}

	n := float64(len(keys))
	var sumX, sumY, sumXY, sumXX float64

	// 遍历所有数据点 (Key 是 x, 数组下标 Index 是 y)
	// 目标：预测 Key 在数组中的 Index
	for i, key := range keys {
		x := float64(key)
		y := float64(i)

		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}

	// 计算斜率和截距
	// 公式：Slope = (N*Σxy - Σx*Σy) / (N*Σx^2 - (Σx)^2)
	denominator := n*sumXX - sumX*sumX
	if denominator == 0 {
		lm.Slope = 0
		lm.Intercept = 0
	} else {
		lm.Slope = (n*sumXY - sumX*sumY) / denominator
		lm.Intercept = (sumY - lm.Slope*sumX) / n
	}
}

// Predict 预测位置
func (lm *LinearModel) Predict(key common.KeyType) int {
	return int(lm.Slope*float64(key) + lm.Intercept)
}
