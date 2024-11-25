package godb

import (
	"os"
	"testing"
)

const InsertTestFile string = "InsertTestFile.dat"

func TestInsert(t *testing.T) {
	td, t1, _, hf, bp, tid := makeTestVars(t)
	hf.insertTuple(&t1, tid)
	hf.insertTuple(&t1, tid)
	bp.CommitTransaction(tid)
	os.Remove(InsertTestFile)
	hf2, _ := NewHeapFile(InsertTestFile, &td, bp)
	if hf2 == nil {
		t.Fatalf("hf was nil")
	}
	tid = NewTID()
	bp.BeginTransaction(tid)
	ins := NewInsertOp(hf2, hf)
	iter, _ := ins.Iterator(tid)
	if iter == nil {
		t.Fatalf("iter was nil")
	}
	var out []*Tuple
	for batch, err := iter(); len(batch) > 0 || err != nil; batch, err = iter() {
		if err != nil {
			t.Fatalf(err.Error())
		}
		out = append(out, batch...)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 tuple, got %d", len(out))
	}
	tup := out[0]
	intField, ok := tup.Fields[0].(IntField)
	if !ok || len(tup.Fields) != 1 || intField.Value != 2 {
		t.Errorf("invalid output tuple")
		return
	}
	bp.CommitTransaction(tid)
	tid = NewTID()
	bp.BeginTransaction(tid)

	cnt := 0
	iter, _ = hf2.Iterator(tid)
	for {
		batch, err := iter()

		if err != nil {
			t.Errorf(err.Error())
		}
		if len(batch) == 0 {
			break
		}
		cnt = cnt + len(batch)
	}
	if cnt != 2 {
		t.Errorf("insert failed, expected 2 tuples, got %d", cnt)
	}
}
