package godb

import (
	"testing"
)


func TestComplexBenchmark1(t *testing.T) {
	// join and then aggregate (count)
	td, hf1, hf2, _, tid := makeBigTableAndVars(t, 10000, false)
	leftField := FieldExpr{td.Fields[0]}
	join, err := NewJoin(hf1, &leftField, hf2, &leftField, 10000)
	if err != nil {
		t.Fatalf("unexpected error initializing join")
	}
	sa := CountAggState{}
	agg := NewAggregator(
		[]AggState{&sa},
		join,
	)
	printTime("ComplexBenchmark1", func() {
		iter, err := agg.Iterator(tid)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		checkCount(t, iter, 1)
	})
}

func TestComplexBenchmark2(t *testing.T) {
	// join, filter, and then aggregate (sum)
	td, hf1, hf2, _, tid := makeBigTableAndVars(t, 10000, false)
	field := FieldExpr{td.Fields[0]}
	join, err := NewJoin(hf1, &field, hf2, &field, 10000)
	if err != nil {
		t.Fatalf("unexpected error initializing join")
	}
	filter, err := NewFilter(
		&ConstExpr{IntField{5000}, IntType},
		OpLt,
		&FieldExpr{FieldType{"age", "t", IntType}},
		join,
	)
	if err != nil {
		t.Fatalf(err.Error())
	}
	sa := SumAggState{}	
	err = sa.Init("sum", &FieldExpr{FieldType{"age", "t2", IntType}})
	if err != nil {
		t.Fatalf(err.Error())
	}
	agg := NewAggregator(
		[]AggState{&sa},
		filter,
	)
	printTime("ComplexBenchmark2", func() {
		iter, err := agg.Iterator(tid)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		checkCount(t, iter, 1)
	})
}

func TestComplexBenchmark3(t *testing.T) {
	// join twice and then join again
	td, hf1, hf2, _, tid := makeBigTableAndVars(t, 30000, false)
	field := FieldExpr{td.Fields[1]}
	join1, err := NewJoin(hf1, &field, hf2, &field, 10000)
	if err != nil {
		t.Fatalf("unexpected error initializing join")
	}
	join2, err := NewJoin(hf1, &field, hf2, &field, 10000)
	if err != nil {
		t.Fatalf("unexpected error initializing join")
	}
	newField := FieldExpr{FieldType{"age", "t2", IntType}}
	joinFinal, err := NewJoin(join1, &newField, join2, &newField, 10000)
	if err != nil {
		t.Fatalf("unexpected error initializing join")
	}
	printTime("ComplexBenchmark3", func() {
		iter, err := joinFinal.Iterator(tid)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		checkCount(t, iter, 30000)
	})
}


func TestComplexBenchmark4(t *testing.T) {
	// filter, project, and then orderby, limit
	_, hf1, _, _, tid := makeBigTableAndVars(t, 30000, true)
	filter, err := NewFilter(
		&ConstExpr{IntField{5000}, IntType},
		OpLt,
		&FieldExpr{FieldType{"age", "", IntType}},
		hf1,
	)
	if err != nil {
		t.Fatalf(err.Error())
	}
	var outNames = []string{"name"}
	exprs := []Expr{&FieldExpr{FieldType{"name", "", StringType}}}
	proj, err := NewProjectOp(exprs, outNames, false, filter)
	if err != nil {
		t.Fatalf(err.Error())
	}
	orderBy, err := NewOrderBy(
		[]Expr{&FieldExpr{FieldType{"name", "", StringType}}},
		proj,
		[]bool{false},
	)
	if err != nil {
		t.Fatalf(err.Error())
	}
	limit := NewLimitOp(&ConstExpr{IntField{10}, IntType}, orderBy)
	
	printTime("ComplexBenchmark4", func() {
		iter, err := limit.Iterator(tid)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		checkCount(t, iter, 10)
	})
}