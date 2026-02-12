package model

//The model defines how Keys are mapped to Positions.

type Model interface {
	Traom(keys []int64) error    //Training
	Predict(Key int64) (pos int) //Predicted position
	ErrorBound() (min, max int)  //Error range(used for final step search)
	SizeInBytes() int            //Model size (used for cost evaluation)
}
