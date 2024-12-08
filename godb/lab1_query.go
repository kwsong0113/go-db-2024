package godb

import (
	"os"
)

/*
computeFieldSum should (1) load the csv file named fileName into a heap file
(see [HeapFile.LoadFromCSV]), (2) compute the sum of the integer field named
sumField string and, (3) return its value as an int.

The supplied csv file is comma delimited and has a header.

If the file doesn't exist, can't be opened, the field doesn't exist, or the
field is not an integer, you should return an error.

Note that when you create a HeapFile, you will need to supply a file name;
you can supply a non-existant file, in which case it will be created.
However, subsequent invocations of this method will result in tuples being
reinserted into this file unless you delete (e.g., with [os.Remove] it before
calling NewHeapFile.

Note that you should NOT pass fileName into NewHeapFile -- fileName is a CSV
file that you should call LoadFromCSV on.
*/
func computeFieldSum(bp *BufferPool, fileName string, td TupleDesc, sumField string) (int, error) {
	nonExistentFileName := fileName[:len(fileName)-4] + "_nonexistent.csv"
	os.Remove(nonExistentFileName)
	heapFile, err := NewHeapFile(nonExistentFileName, &td, bp)
	if err != nil {
		return 0, err
	}
	file, err := os.Open(fileName)
	if err != nil {
		return 0, err
	}
	err = heapFile.LoadFromCSV(file, true, ",", false)
	if err != nil {
		return 0, err
	}
	iter, err := heapFile.Iterator(0)
	if err != nil {
		return 0, err
	}
	sum := 0
	for {
		batch, err := iter()
		if err != nil {
			return 0, err
		}
		if len(batch) == 0 {
			break
		}
		for _, tuple := range batch {
			tuple, err = tuple.project([]FieldType{{sumField, "", IntType}})
			if err != nil {
				return 0, err
			}
			sum += int(tuple.Fields[0].(IntField).Value)
		}
	}
	return sum, nil
}
