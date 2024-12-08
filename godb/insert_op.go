package godb

type InsertOp struct {
	// TODO: some code goes here
	insertFile DBFile
	child Operator
}

// Construct an insert operator that inserts the records in the child Operator
// into the specified DBFile.
func NewInsertOp(insertFile DBFile, child Operator) *InsertOp {
	// TODO: some code goes here
	return &InsertOp{insertFile: insertFile, child: child}
}

// The insert TupleDesc is a one column descriptor with an integer field named "count"
func (i *InsertOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	fts := []FieldType{{"count", "", IntType}}
	return &TupleDesc{Fields: fts}
}

// Return an iterator function that inserts all of the tuples from the child
// iterator into the DBFile passed to the constuctor and then returns a
// one-field tuple with a "count" field indicating the number of tuples that
// were inserted.  Tuples should be inserted using the [DBFile.insertTuple]
// method.
func (iop *InsertOp) Iterator(tid TransactionID) (func() ([]*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := iop.child.Iterator(tid)
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
				err = iop.insertFile.insertTuple(t, tid)
				if err != nil {
					return nil, err
				}
				count++
			}
		}
		done = true
		return []*Tuple{{*iop.Descriptor(), []DBValue{IntField{Value: count}}, nil}}, nil
	}), nil
}
