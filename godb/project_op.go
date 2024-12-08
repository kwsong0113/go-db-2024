package godb

type Project struct {
	selectFields []Expr // required fields for parser
	outputNames  []string
	child        Operator
	// You may want to add additional fields here
	// TODO: some code goes here
	distinct bool
}

// Construct a projection operator. It saves the list of selected field, child,
// and the child op. Here, selectFields is a list of expressions that represents
// the fields to be selected, outputNames are names by which the selected fields
// are named (should be same length as selectFields; throws error if not),
// distinct is for noting whether the projection reports only distinct results,
// and child is the child operator.
func NewProjectOp(selectFields []Expr, outputNames []string, distinct bool, child Operator) (Operator, error) {
	// TODO: some code goes here
	return &Project{selectFields: selectFields, outputNames: outputNames, child: child, distinct: distinct}, nil
}

// Return a TupleDescriptor for this projection. The returned descriptor should
// contain fields for each field in the constructor selectFields list with
// outputNames as specified in the constructor.
//
// HINT: you can use expr.GetExprType() to get the field type
func (p *Project) Descriptor() *TupleDesc {
	// TODO: some code goes here
	fts := make([]FieldType, len(p.selectFields))
	for i, expr := range p.selectFields {
		ft := expr.GetExprType()
		ft.Fname = p.outputNames[i]
		fts[i] = ft
	}
	return &TupleDesc{Fields: fts}
}

// Project operator implementation. This function should iterate over the
// results of the child iterator, projecting out the fields from each tuple. In
// the case of distinct projection, duplicate tuples should be removed. To
// implement this you will need to record in some data structure with the
// distinct tuples seen so far. Note that support for the distinct keyword is
// optional as specified in the lab 2 assignment.
func (p *Project) Iterator(tid TransactionID) (func() ([]*Tuple, error), error) {
	// TODO: some code goes here
	distinctTuples := make(map[any]bool)
	childIter, err := p.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	ftsToProject := make([]FieldType, len(p.selectFields))
	desc := p.Descriptor()
	for i, expr := range p.selectFields {
		ftsToProject[i] = expr.GetExprType()
	}
	cachedBatch := make([]*Tuple, 0)
	return validate(func() ([]*Tuple, error) {
		currentBatch := make([]*Tuple, 0)
		checkedCachedBatch := false
		for batch, err := cachedBatch, error(nil); !checkedCachedBatch || len(batch) > 0 || err != nil; batch, err = childIter() {
			if err != nil {
				return nil, err
			}
			if !checkedCachedBatch {
				checkedCachedBatch = true
				cachedBatch = make([]*Tuple, 0)
			}
			for idx, t := range batch {
				fields := make([]DBValue, len(p.selectFields))
				for i, expr := range p.selectFields {
					outf, err := expr.EvalExpr(t)
					if err != nil {
						return nil, err
					}
					fields[i] = outf
				}
				t = &Tuple{*desc, fields, nil}
				if p.distinct {
					if distinctTuples[t.tupleKey()] {
						continue
					} else {
						distinctTuples[t.tupleKey()] = true
					}
				}
				t.Desc = *desc
				currentBatch = append(currentBatch, t)
				if len(currentBatch) == BatchSize {
					if idx+1 < len(batch) {
						cachedBatch = batch[idx+1:]
					} else {
						cachedBatch = nil
					}
					return currentBatch, nil
				}
			}
		}
		cachedBatch = nil
		return currentBatch, nil
	}), nil
}
