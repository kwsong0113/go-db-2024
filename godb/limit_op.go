package godb

type LimitOp struct {
	// Required fields for parser
	child     Operator
	limitTups Expr
	// Add additional fields here, if needed
}

// Construct a new limit operator. lim is how many tuples to return and child is
// the child operator.
func NewLimitOp(lim Expr, child Operator) *LimitOp {
	return &LimitOp{child: child, limitTups: lim}
}

// Return a TupleDescriptor for this limit.
func (l *LimitOp) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return l.child.Descriptor()
}

// Limit operator implementation. This function should iterate over the results
// of the child iterator, and limit the result set to the first [lim] tuples it
// sees (where lim is specified in the constructor).
func (l *LimitOp) Iterator(tid TransactionID) (func() ([]*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := l.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	count := 0
	var limit int
	hasLimit := false
	return func() ([]*Tuple, error) {
		if hasLimit && count >= limit {
			return nil, nil
		}

		// here it's assumed that childIter correctly returns the batch capped at BatchSize
		batch, err := childIter()
		if err != nil {
			return nil, err
		}
		if len(batch) == 0 {
			return nil, nil
		}
		if !hasLimit {
			limitTup, err := l.limitTups.EvalExpr(batch[0])
			if err != nil {
				return nil, err
			}
			limit = int(limitTup.(IntField).Value)
			hasLimit = true
		}
		if limit - count < len(batch) {
			batch = batch[:limit - count]
		}
		count += len(batch)
		return batch, nil		
	}, nil
}
