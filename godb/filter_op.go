package godb

type Filter struct {
	op    BoolOp
	left  Expr
	right Expr
	child Operator
}

// Construct a filter operator on ints.
func NewFilter(constExpr Expr, op BoolOp, field Expr, child Operator) (*Filter, error) {
	return &Filter{op, field, constExpr, child}, nil
}

// Return a TupleDescriptor for this filter op.
func (f *Filter) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.child.Descriptor()
}

// Filter operator implementation. This function should iterate over the results
// of the child iterator and return a tuple if it satisfies the predicate.
//
// HINT: you can use [types.evalPred] to compare two values.
func (f *Filter) Iterator(tid TransactionID) (func() (*Tuple, error), error) {
	// TODO: some code goes here
	iter, err := f.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	return func() (*Tuple, error) {
		for {
			t, err := iter()
			if err != nil {
				return nil, err
			}
			if t == nil {
				return nil, nil
			}
			left, err := f.left.EvalExpr(t)
			if err != nil {
				return nil, err
			}
			right, err := f.right.EvalExpr(t)
			if err != nil {
				return nil, err
			}
			if left.EvalPred(right, f.op) {
				return t, nil
			}
		}
	}, nil
}