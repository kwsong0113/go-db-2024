package godb

type Aggregator struct {
	// Expressions that when applied to tuples from the child operators,
	// respectively, return the value of the group by key tuple
	groupByFields []Expr

	// Aggregation states that serves as a template as to which types of
	// aggregations in which order are to be computed for every group.
	newAggState []AggState

	child Operator // the child operator for the inputs to aggregate
}

type AggType int

const (
	IntAggregator    AggType = iota
	StringAggregator AggType = iota
)

const DefaultGroup int = 0 // for handling the case of no group-by

// Construct an aggregator with a group-by.
func NewGroupedAggregator(emptyAggState []AggState, groupByFields []Expr, child Operator) *Aggregator {
	return &Aggregator{groupByFields, emptyAggState, child}
}

// Construct an aggregator with no group-by.
func NewAggregator(emptyAggState []AggState, child Operator) *Aggregator {
	return &Aggregator{nil, emptyAggState, child}
}

// Return a TupleDescriptor for this aggregation.
//
// If the aggregator has no group-by, the returned descriptor should contain the
// union of the fields in the descriptors of the aggregation states. If the
// aggregator has a group-by, the returned descriptor will additionally start
// with the group-by fields, and then the aggregation states descriptors like
// that without group-by.
//
// HINT: for groupByFields, you can use [Expr.GetExprType] to get the FieldType.
//
// HINT: use [TupleDesc.merge] to merge the two [TupleDesc]s.
func (a *Aggregator) Descriptor() *TupleDesc {
	// TODO: some code goes here
	fts := make([]FieldType, 0)
	if a.groupByFields != nil {
		for _, expr := range a.groupByFields {
			fts = append(fts, expr.GetExprType())
		}
	}
	td := TupleDesc{Fields: fts}
	for _, as := range a.newAggState {
		td = *td.merge(as.GetTupleDesc())
	}
	return &td
}

// Returns an iterator over the results of the aggregate. The aggregate should
// be the result of aggregating each group's tuples and the iterator should
// iterate through each group's result. In the case where there is no group-by,
// the iterator simply iterates through only one tuple, representing the
// aggregation of all child tuples.
func (a *Aggregator) Iterator(tid TransactionID) (func() ([]*Tuple, error), error) {
	// the child iterator
	childIter, err := a.child.Iterator(tid)
	if err != nil {
		return nil, err
	}
	if childIter == nil {
		return nil, GoDBError{MalformedDataError, "child iter unexpectedly nil"}
	}

	// the map that stores the aggregation state of each group
	aggState := make(map[any]*[]AggState)
	if a.groupByFields == nil {
		var newAggState []AggState
		for _, as := range a.newAggState {
			copy := as.Copy()
			if copy == nil {
				return nil, GoDBError{MalformedDataError, "aggState Copy unexpectedly returned nil"}
			}
			newAggState = append(newAggState, copy)
		}

		aggState[DefaultGroup] = &newAggState
	}

	// the list of group key tuples
	var groupByList []*Tuple
	// the iterator for iterating thru the finalized aggregation results for each group
	var finalizedIter func() ([]*Tuple, error)

	return func() ([]*Tuple, error) {
		// iterates thru all child tuples
		for batch, err := childIter(); len(batch) > 0 || err != nil; batch, err = childIter() {
			if err != nil {
				return nil, err
			}
			for _, t := range batch {
				if a.groupByFields == nil { // adds tuple to the aggregation in the case of no group-by
					for i := 0; i < len(a.newAggState); i++ {
						(*aggState[DefaultGroup])[i].AddTuple(t)
					}
				} else { // adds tuple to the aggregation with grouping
					keygenTup, err := extractGroupByKeyTuple(a, t)
					if err != nil {
						return nil, err
					}

					key := keygenTup.tupleKey()
					if aggState[key] == nil {
						asNew := make([]AggState, len(a.newAggState))
						aggState[key] = &asNew
						groupByList = append(groupByList, keygenTup)
					}

					addTupleToGrpAggState(a, t, aggState[key])
				}
			}
		}

		if finalizedIter == nil { // builds the iterator for iterating thru the finalized aggregation results for each group
			if a.groupByFields == nil {
				var tup *Tuple
				for i := 0; i < len(a.newAggState); i++ {
					newTup := (*aggState[DefaultGroup])[i].Finalize()
					tup = joinTuples(tup, newTup)
				}
				finalizedIter = func() ([]*Tuple, error) { return nil, nil }
				return []*Tuple{tup}, nil
			} else {
				finalizedIter = getFinalizedTuplesIterator(a, groupByList, aggState)
			}
		}
		return finalizedIter()
	}, nil
}

// Given a tuple t from a child iterator, return a tuple that identifies t's
// group. The returned tuple should contain the fields from the groupByFields
// list passed into the aggregator constructor. The ith field can be extracted
// from the supplied tuple using the EvalExpr method on the ith expression of
// groupByFields.
//
// If there is any error during expression evaluation, return the error.
func extractGroupByKeyTuple(a *Aggregator, t *Tuple) (*Tuple, error) {
	// TODO: some code goes here
	fts := make([]FieldType, 0)
	fields := make([]DBValue, 0)
	for _, expr := range a.groupByFields {
		val, err := expr.EvalExpr(t)
		if err != nil {
			return nil, err
		}
		fts = append(fts, expr.GetExprType())
		fields = append(fields, val)
	}
	return &Tuple{TupleDesc{Fields: fts}, fields, nil}, nil
}

// Given a tuple t from child and (a pointer to) the array of partially computed
// aggregates grpAggState, add t into all partial aggregations using
// [AggState.AddTuple]. If any of the array elements is of grpAggState is null
// (i.e., because this is the first invocation of this method, create a new
// aggState using [aggState.Copy] on appropriate element of the a.newAggState
// field and add the new aggState to grpAggState.
func addTupleToGrpAggState(a *Aggregator, t *Tuple, grpAggState *[]AggState) {
	// TODO: some code goes here
	for i, as := range *grpAggState {
		if as == nil {
			(*grpAggState)[i] = a.newAggState[i].Copy()
		}
		(*grpAggState)[i].AddTuple(t)
	}
}

// Given that all child tuples have been added, return an iterator that iterates
// through the finalized aggregate result one group at a time. The returned
// tuples should be structured according to the TupleDesc returned from the
// Descriptor() method.
//
// HINT: you can call [aggState.Finalize] to get the field for each AggState.
// Then, you should get the groupByTuple and merge it with each of the AggState
// tuples using the joinTuples function in tuple.go you wrote in lab 1.
func getFinalizedTuplesIterator(a *Aggregator, groupByList []*Tuple, aggState map[any]*[]AggState) func() ([]*Tuple, error) {
	// TODO: some code goes here
	groupIdx := 0
	return func() ([]*Tuple, error) {
		// TODO: some code goes here
		var batch []*Tuple
		for groupIdx < len(groupByList) && len(batch) < BatchSize {
			groupByTup := groupByList[groupIdx]
			for _, as := range *aggState[groupByTup.tupleKey()] {
				groupByTup = joinTuples(groupByTup, as.Finalize())
			}
			batch = append(batch, groupByTup)
			groupIdx++
		}
		return batch, nil
	}
}
