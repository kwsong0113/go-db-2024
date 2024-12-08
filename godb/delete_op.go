package godb

type DeleteOp struct {
	// TODO: some code goes here
	deleteFile DBFile
	child Operator
}

// Construct a delete operator. The delete operator deletes the records in the
// child Operator from the specified DBFile.
func NewDeleteOp(deleteFile DBFile, child Operator) *DeleteOp {
	// TODO: some code goes here
	return &DeleteOp{deleteFile: deleteFile, child: child}
}

// The delete TupleDesc is a one column descriptor with an integer field named
// "count".
func (i *DeleteOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	fts := []FieldType{{"count", "", IntType}}
	return &TupleDesc{Fields: fts}

}

// Return an iterator that deletes all of the tuples from the child iterator
// from the DBFile passed to the constructor and then returns a one-field tuple
// with a "count" field indicating the number of tuples that were deleted.
// Tuples should be deleted using the [DBFile.deleteTuple] method.
func (dop *DeleteOp) Iterator(tid TransactionID) (func() ([]*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := dop.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	done := false
	return validate(func() ([]*Tuple, error) {
		if done {
			return nil, nil
		}
		count := int64(0)
		for {
			batch, err := childIter()
			if err != nil {
				return nil, err
			}
			if len(batch) == 0 {
				break
			}
			for _, t := range batch {
				err = dop.deleteFile.deleteTuple(t, tid)
				if err != nil {
					return nil, err
				}
				count++
			}
		}
		done = true
		return []*Tuple{{*dop.Descriptor(), []DBValue{IntField{Value: count}}, nil}}, nil
	}), nil
}
