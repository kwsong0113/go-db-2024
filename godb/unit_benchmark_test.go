package godb

import (
	"fmt"
	"testing"
	"time"
)

func checkCount(t *testing.T, iter func() ([]*Tuple, error), expectedCount int) {
	cnt := 0
	for {
		batch, _ := iter()
		cnt += len(batch)
		if len(batch) == 0 {
			break
		}
	}
	if cnt != expectedCount {
		t.Errorf("expected %d tuples, got %d", expectedCount, cnt)
	}
}

func printTime(name string, f func()) {
	start := time.Now()
	f()
	fmt.Printf("%s: %.3fms\n", name, float64(time.Since(start).Seconds())*1000)
}

func TestFilterUnitBenchmark(t *testing.T) {
	_, hf1, _, _, tid := makeBigTableAndVars(t, TableSize, true)
	filter, err := NewFilter(
		&ConstExpr{IntField{int64(TableSize / 2)}, IntType},
		OpLt,
		&FieldExpr{FieldType{"age", "", IntType}},
		hf1,
	)
	if err != nil {
		t.Fatalf(err.Error())
	}
	printTime("Filter", func() {
		iter, err := filter.Iterator(tid)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		checkCount(t, iter, TableSize / 2)
	})
}

func TestOrderByUnitBenchmark(t *testing.T) {
	_, hf1, _, _, tid := makeBigTableAndVars(t, TableSize, true)
	orderBy, err := NewOrderBy(
		[]Expr{&FieldExpr{FieldType{"age", "", IntType}}},
		hf1,
		[]bool{false},
	)
	if err != nil {
		t.Fatalf(err.Error())
	}
	printTime("OrderBy", func() {
		iter, err := orderBy.Iterator(tid)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		checkCount(t, iter, TableSize)
	})
}

func TestLimitUnitBenchmark(t *testing.T) {
	_, hf1, _, _, tid := makeBigTableAndVars(t, TableSize, true)
	limit := NewLimitOp(&ConstExpr{IntField{int64(TableSize / 2)}, IntType}, hf1)
	printTime("Limit", func() {
		iter, err := limit.Iterator(tid)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		checkCount(t, iter, TableSize / 2)
	})
}

func TestProjectUnitBenchmark(t *testing.T) {
	_, hf1, _, _, tid := makeBigTableAndVars(t, TableSize, true)
	var outNames = []string{"name"}
	exprs := []Expr{&FieldExpr{FieldType{"name", "", StringType}}}
	proj, err := NewProjectOp(exprs, outNames, false, hf1)
	if err != nil {
		t.Fatalf(err.Error())
	}
	printTime("Project", func() {
		iter, err := proj.Iterator(tid)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if iter == nil {
			t.Fatalf("Iterator was nil")
		}
		checkCount(t, iter, TableSize)
	})
}

func TestJoinUnitBenchmark(t *testing.T) {
	td, hf1, hf2, _, tid := makeBigTableAndVars(t, TableSize, false)
	leftField := FieldExpr{td.Fields[1]}
	join, err := NewJoin(hf1, &leftField, hf2, &leftField, TableSize)
	if err != nil {
		t.Fatalf("unexpected error initializing join")
	}
	printTime("Join", func() {
		iter, err := join.Iterator(tid)
		if err != nil {
			t.Fatalf(err.Error())
		}
		if iter == nil {
			t.Fatalf("iter was nil")
		}
		checkCount(t, iter, TableSize)
	})
}

func TestAggUnitBenchmark(t *testing.T) {
	_, hf1, _, _, tid := makeBigTableAndVars(t, TableSize, true)
	// sum aggregation
	sa := SumAggState{}
	err := sa.Init("sum", &FieldExpr{FieldType{"age", "", IntType}})
	if err != nil {
		t.Fatalf(err.Error())
	}
	agg := NewAggregator(
		[]AggState{&sa},
		hf1,
	)
	printTime("Agg", func() {
		iter, _ := agg.Iterator(tid)
		checkCount(t, iter, 1)
	})
}
