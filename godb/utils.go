package godb

// Modify the iterator to check the batch size
// every time it is called, and return an error if the batch size is exceeded.
func validate(iterator func() ([]*Tuple, error)) func() ([]*Tuple, error) {
	newIterator := func() ([]*Tuple, error) {
		batch, err := iterator()
		if err != nil {
			return nil, err
		}
		if len(batch) > BatchSize {
			return nil, GoDBError{BatchSizeExceededError, "batch size exceeded"}
		}
		return batch, nil
	}
	return newIterator
}