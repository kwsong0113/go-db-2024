package godb

import "testing"

func TestNotFullEdgeCases(t *testing.T) {
	_, hf1, _, _, tid := makeBigTableAndVars(t, 1000, false)

	// check if the filter works when returning small number of tuples
	// (smaller than the batch size or even empty)
	for num_tuples := 0; num_tuples < BatchSize; num_tuples++ {
		filter, err := NewFilter(
			&ConstExpr{IntField{int64(num_tuples)}, IntType},
			OpLt,
			&FieldExpr{FieldType{"age", "", IntType}},
			hf1,
		)
		if err != nil {
			t.Fatalf(err.Error())
		}
		iter, err := filter.Iterator(tid)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		checkBatchSizeAndCount(t, iter, num_tuples)
	}
}

func TestJoinEdgeCases(t *testing.T) {
	td, hf1, hf2, _, tid := makeBigTableAndVars(t, 1000, false)
	// joining different fields should return 0 tuples
	leftField := FieldExpr{td.Fields[0]}
	rightField := FieldExpr{td.Fields[1]}
	join, err := NewJoin(hf1, &leftField, hf2, &rightField, 10000)
	if err != nil {
		t.Fatalf("unexpected error initializing join")
	}
	iter, err := join.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	checkBatchSizeAndCount(t, iter, 0)

	// joining with self
	leftField = FieldExpr{td.Fields[0]}
	join, err = NewJoin(hf1, &leftField, hf1, &leftField, 10000)
	if err != nil {
		t.Fatalf("unexpected error initializing join")
	}
	iter, err = join.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	checkBatchSizeAndCount(t, iter, 500000)
}

func TestEndOfIteratorEdgeCases(t *testing.T) {
	_, hf1, _, _, tid := makeBigTableAndVars(t, 1000, false)

	// check if the end of the iterator is reached correctly
	// by returning 0 tuples or nil
	// iterate over all tuples
	iter, err := hf1.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	cnt := 0
	for {
		batch, err := iter()
		if err != nil {
			t.Fatalf(err.Error())
		}
		if len(batch) == 0 { // end of iterator
			break
		}
		cnt += len(batch)
	}
	if cnt != 1000 {
		t.Errorf("expected 1000 tuples, got %d", cnt)
	}
}