package godb

import "sort"

type OrderBy struct {
	orderBy []Expr // OrderBy should include these two fields (used by parser)
	child   Operator
	// TODO: You may want to add additional fields here
	ascending []bool
}

// Construct an order by operator. Saves the list of field, child, and ascending
// values for use in the Iterator() method. Here, orderByFields is a list of
// expressions that can be extracted from the child operator's tuples, and the
// ascending bitmap indicates whether the ith field in the orderByFields list
// should be in ascending (true) or descending (false) order.
func NewOrderBy(orderByFields []Expr, child Operator, ascending []bool) (*OrderBy, error) {
	// TODO: some code goes here
	return &OrderBy{orderBy: orderByFields, child: child, ascending: ascending}, nil
 
}

// Return the tuple descriptor.
//
// Note that the order by just changes the order of the child tuples, not the
// fields that are emitted.
func (o *OrderBy) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return o.child.Descriptor()
}

type lessFunc func(t1, t2 *Tuple) bool
type multiSorter struct {
	tuples []*Tuple
	less   []lessFunc
}

func (ms *multiSorter) Len() int {
	return len(ms.tuples)
}

func (ms *multiSorter) Swap(i, j int) {
	ms.tuples[i], ms.tuples[j] = ms.tuples[j], ms.tuples[i]
}

func (ms *multiSorter) Less(i, j int) bool {
	t1, t2 := ms.tuples[i], ms.tuples[j]
	for _, less := range ms.less {
		if less(t1, t2) {
			return true
		} else if less(t2, t1) {
			return false
		}
	}
	return ms.less[len(ms.less)-1](t1, t2)
}

func createLessFunc(field Expr, ascending bool) lessFunc {
	return func(t1, t2 *Tuple) bool {
		f1, err1 := field.EvalExpr(t1)
		if err1 != nil {
			return false
		}
		f2, err2 := field.EvalExpr(t2)
		if err2 != nil {
			return false
		}
		if ascending {
			return f1.EvalPred(f2, OpLt)
		} else {
			return f2.EvalPred(f1, OpLt)
		}
	}
}

func (o *OrderBy) sort(tuples []*Tuple) {
	less := make([]lessFunc, len(o.orderBy))
	for i, field := range o.orderBy {
		less[i] = createLessFunc(field, o.ascending[i])
	}
	ms := &multiSorter{
		tuples: tuples,
		less:   less,
	}
	sort.Sort(ms)
}




// Return a function that iterates through the results of the child iterator in
// ascending/descending order, as specified in the constructor.  This sort is
// "blocking" -- it should first construct an in-memory sorted list of results
// to return, and then iterate through them one by one on each subsequent
// invocation of the iterator function.
//
// Although you are free to implement your own sorting logic, you may wish to
// leverage the go sort package and the [sort.Sort] method for this purpose. To
// use this you will need to implement three methods: Len, Swap, and Less that
// the sort algorithm will invoke to produce a sorted list. See the first
// example, example of SortMultiKeys, and documentation at:
// https://pkg.go.dev/sort
func (o *OrderBy) Iterator(tid TransactionID) (func() ([]*Tuple, error), error) {
	// TODO: some code goes here
	childIter, err := o.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	var tuples []*Tuple
	idx := 0

	return validate(func() ([]*Tuple, error) {
		if tuples == nil {
			tuples = make([]*Tuple, 0)
			for {
				batch, err := childIter()
				if err != nil {
					return nil, err
				}
				if len(batch) == 0 {
					break
				}
				tuples = append(tuples, batch...)
			}
			o.sort(tuples)
		}
		if idx >= len(tuples) {
			return nil, nil
		}
		currBatchSize := min(BatchSize, len(tuples) - idx)
		idx += currBatchSize
		return tuples[idx - currBatchSize : idx], nil
	}), nil
}