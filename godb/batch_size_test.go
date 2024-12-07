package godb

import (
	"fmt"
	"os"
	"testing"
)

func makeBigTestFile(t *testing.T, bufferPoolSize int) (*BufferPool, *HeapFile, *HeapFile) {
	os.Remove(TestingFile)
	os.Remove(TestingFile2)

	bp, c, err := MakeTestDatabase(bufferPoolSize, "catalog.txt")
	if err != nil {
		t.Fatalf(err.Error())
	}

	td, _, _ := makeTupleTestVars()
	tbl1, err := c.addTable("test", td)
	if err != nil {
		t.Fatalf(err.Error())
	}
	tbl2, err := c.addTable("test2", td)
	if err != nil {
		t.Fatalf(err.Error())
	}
	return bp, tbl1.(*HeapFile), tbl2.(*HeapFile)
}

// size is the size of each table
func makeBigTableAndVars(t *testing.T, size int) (TupleDesc, *HeapFile, *HeapFile, *BufferPool, TransactionID) {
	bp, hf1, hf2 := makeBigTestFile(t, 500)
	td := TupleDesc{Fields: []FieldType{
		{Fname: "name", Ftype: StringType},
		{Fname: "age", Ftype: IntType},
	}}
	tid := NewTID()
	bp.BeginTransaction(tid)
	for i := 0; i < size; i++ {
		if i > 0 && i % 5000 == 0 {
			bp.FlushAllPages()
			bp.CommitTransaction(tid)
			tid = NewTID()
			bp.BeginTransaction(tid)
		}
		name := ""
		if i % 2 == 0 {
			name = "sam"
		} else {
			name = "george"
		}
		tup := Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{name},
				IntField{int64(i)},
			}}
		err := hf1.insertTuple(&tup, tid)
		if err != nil {
			t.Fatalf(fmt.Sprintf("Error inserting tuple1: %s", err.Error()))
		}
		if i % 4 == 0 || i % 4 == 1 {
			name = "sam"
		} else {
			name = "george"
		}
		tup = Tuple{
			Desc: td,
			Fields: []DBValue{
				StringField{name},
				IntField{int64(i)},
			}}
		err = hf2.insertTuple(&tup, tid)
		if err != nil {
			t.Fatalf(fmt.Sprintf("Error inserting tuple2: %s", err.Error()))
		}
	}
	bp.FlushAllPages()
	bp.CommitTransaction(tid)
	tid = NewTID()
	bp.BeginTransaction(tid)
	return td, hf1, hf2, bp, tid
}

func checkBatchSizeAndCount(t *testing.T, iter func() ([]*Tuple, error), expectedCount int) {
	cnt := 0
	isLastBatch := false
	for {
		batch, _ := iter()
		if len(batch) == 0 {
			break
		} else if len(batch) < BatchSize {
			if isLastBatch {
				t.Errorf("batch size is less than BatchSize not on last batch")
			}
			isLastBatch = true
		}
		cnt += len(batch)
	}
	if cnt != expectedCount {
		t.Errorf("expected %d tuples, got %d", expectedCount, cnt)
	}
}

func TestFilterBatch(t *testing.T) {
	_, hf1, _, _, tid := makeBigTableAndVars(t, 10000)
	filter, err := NewFilter(
		&ConstExpr{IntField{25}, IntType},
		OpGt,
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
	checkBatchSizeAndCount(t, iter, 9974)
}

func TestJoinBatch(t *testing.T) {
	td, hf1, hf2, _, tid := makeBigTableAndVars(t, 1000)
	leftField := FieldExpr{td.Fields[0]}
	join, err := NewJoin(hf1, &leftField, hf2, &leftField, 10000)
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
	checkBatchSizeAndCount(t, iter, 500000)
}

func TestProjectBatch(t *testing.T) {
	td, hf1, _, _, tid := makeBigTableAndVars(t, 10000)
	var outNames = []string{"name"}
	exprs := []Expr{&FieldExpr{td.Fields[0]}}
	proj, _ := NewProjectOp(exprs, outNames, false, hf1)
	if proj == nil {
		t.Fatalf("project was nil")
	}
	iter, _ := proj.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	checkBatchSizeAndCount(t, iter, 10000)
}

func TestLimitBatch(t *testing.T) {
	_, hf1, _, _, tid := makeBigTableAndVars(t, 10000)
	limit := NewLimitOp(&ConstExpr{IntField{8000}, IntType}, hf1)
	iter, err := limit.Iterator(tid)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if iter == nil {
		t.Fatalf("Iterator was nil")
	}
	checkBatchSizeAndCount(t, iter, 8000)
}