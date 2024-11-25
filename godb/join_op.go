package godb


type EqualityJoin struct {
	// Expressions that when applied to tuples from the left or right operators,
	// respectively, return the value of the left or right side of the join
	leftField, rightField Expr

	left, right *Operator // Operators for the two inputs of the join

	// The maximum number of records of intermediate state that the join should
	// use (only required for optional exercise).
	maxBufferSize int
}

// Constructor for a join of integer expressions.
//
// Returns an error if either the left or right expression is not an integer.
func NewJoin(left Operator, leftField Expr, right Operator, rightField Expr, maxBufferSize int) (*EqualityJoin, error) {
	return &EqualityJoin{leftField, rightField, &left, &right, maxBufferSize}, nil
}

// Return a TupleDesc for this join. The returned descriptor should contain the
// union of the fields in the descriptors of the left and right operators.
//
// HINT: use [TupleDesc.merge].
func (hj *EqualityJoin) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return (*hj.left).Descriptor().merge((*hj.right).Descriptor())
}

// Join operator implementation. This function should iterate over the results
// of the join. The join should be the result of joining joinOp.left and
// joinOp.right, applying the joinOp.leftField and joinOp.rightField expressions
// to the tuples of the left and right iterators respectively, and joining them
// using an equality predicate.
//
// HINT: When implementing the simple nested loop join, you should keep in mind
// that you only iterate through the left iterator once (outer loop) but iterate
// through the right iterator once for every tuple in the left iterator (inner
// loop).
//
// HINT: You can use [Tuple.joinTuples] to join two tuples.
//
// OPTIONAL EXERCISE: the operator implementation should not use more than
// maxBufferSize records, and should pass the testBigJoin test without timing
// out. To pass this test, you will need to use something other than a nested
// loops join.
func (joinOp *EqualityJoin) Iterator(tid TransactionID) (func() ([]*Tuple, error), error) {
	// TODO: some code goes here
	// -----------------------Simple Nested Loop Join-----------------------
	// NOTE: Simple Nested Loop Join here is outdated,
	// only works for previous "tuple-at-a-time" Iterator
	// leftIter, err := (*joinOp.left).Iterator(tid)
	// if err != nil {
	// 	return nil, err
	// }
	// left, err := leftIter()
	// if err != nil {
	// 	return nil, err
	// }
	// rightIter, err := (*joinOp.right).Iterator(tid)
	// if err != nil {
	// 	return nil, err
	// }
	// return func() (*Tuple, error) {
	// 	for {
	// 		if left == nil {
	// 			return nil, nil
	// 		}
	// 		right, err := rightIter()
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		if right == nil {
	// 			left, err = leftIter()
	// 			if err != nil {
	// 				return nil, err
	// 			}
	// 			rightIter, err = (*joinOp.right).Iterator(tid)
	// 			if err != nil {
	// 				return nil, err
	// 			}
	// 			continue
	// 		}
	// 		leftVal, err := joinOp.leftField.EvalExpr(left)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		rightVal, err := joinOp.rightField.EvalExpr(right)
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		if leftVal.EvalPred(rightVal, OpEq) {
	// 			return joinTuples(left, right), nil
	// 		}
	// 	}
	// }, nil
	// -----------------------Block Hash Join-----------------------

	leftIter, err := (*joinOp.left).Iterator(tid)
	if err != nil {
		return nil, err
	}
	rightIter, err := (*joinOp.right).Iterator(tid)
	if err != nil {
		return nil, err
	}

	var cachedBatch []*Tuple // batch of right tuples to be included in the next hash table

	// utility function to build block hash table that does not exceed maxBufferSize
	buildHashTable := func() (map[DBValue][]*Tuple, error) {
		hashTable := make(map[DBValue][]*Tuple)
		count := 0
		checkedCachedBatch := false
		for batch, err := cachedBatch, error(nil); !checkedCachedBatch || len(batch) > 0 || err != nil; batch, err = rightIter() {
			if !checkedCachedBatch {
				checkedCachedBatch = true
				cachedBatch = nil
			}
			if err != nil {
				return nil, err
			}
			for _, t := range batch {
				key, err := joinOp.rightField.EvalExpr(t)
				if err != nil {
					return nil, err
				}
				if count < joinOp.maxBufferSize {
					hashTable[key] = append(hashTable[key], t)
				} else {
					cachedBatch = append(cachedBatch, t)
				}
				count++
			}
			if count >= joinOp.maxBufferSize {
				break
			}
		}
		return hashTable, nil
	}

	var hashTable map[DBValue][]*Tuple

	var left []*Tuple
	var right []*Tuple
	var leftIdx int
	var rightIdx int

	return func() ([]*Tuple, error) {
		var batch []*Tuple
		for {
			if hashTable == nil {
				hashTable, err = buildHashTable()
				if err != nil {
					return nil, err
				}
			}
			if len(hashTable) == 0 {
				break
			}
			if len(left) == 0 {
				left, err = leftIter()
				leftIdx = 0
				if err != nil {
					return nil, err
				}
				if len(left) == 0 {
					leftIter, err = (*joinOp.left).Iterator(tid)
					if err != nil {
						return nil, err
					}
					hashTable = nil
					continue
				}
			}
			for leftIdx < len(left) {
				leftTup := left[leftIdx]
				if len(right) == 0 {
					leftVal, err := joinOp.leftField.EvalExpr(leftTup)
					if err != nil {
						return nil, err
					}
					right = hashTable[leftVal]
					rightIdx = 0
				}
				for rightIdx < len(right) {
					rightTup := right[rightIdx]
					rightIdx++
					batch = append(batch, joinTuples(leftTup, rightTup))
					if len(batch) == BatchSize {
						return batch, nil
					}
				}
				leftIdx++
				right = nil
			}
			left = nil
		}
		return batch, nil
	}, nil
}
