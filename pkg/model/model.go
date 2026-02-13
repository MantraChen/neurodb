package model

type Model interface {
	Traom(keys []int64) error
	Predict(Key int64) (pos int)
	ErrorBound() (min, max int)
	SizeInBytes() int
}
