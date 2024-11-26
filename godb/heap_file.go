package godb

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// A HeapFile is an unordered collection of tuples.
//
// HeapFile is a public class because external callers may wish to instantiate
// database tables using the method [LoadFromCSV]
type HeapFile struct {
	// TODO: some code goes here
	// HeapFile should include the fields below;  you may want to add
	// additional fields
	backingFile string
	desc *TupleDesc
	bufPool *BufferPool
	mu sync.Mutex
}

// Create a HeapFile.
// Parameters
// - fromFile: backing file for the HeapFile.  May be empty or a previously created heap file.
// - td: the TupleDesc for the HeapFile.
// - bp: the BufferPool that is used to store pages read from the HeapFile
// May return an error if the file cannot be opened or created.
func NewHeapFile(fromFile string, td *TupleDesc, bp *BufferPool) (*HeapFile, error) {
	// TODO: some code goes here
	_, err := os.Stat(fromFile)
    if os.IsNotExist(err) {
		file, err := os.Create(fromFile)
		if err != nil {
			return nil, err
		}
		file.Close()
	} else if err != nil {
		return nil, err
	}
	return &HeapFile{
		backingFile: fromFile,
		desc: td,
		bufPool: bp,
	}, nil
}

// Return the name of the backing file
func (f *HeapFile) BackingFile() string {
	// TODO: some code goes here
	return f.backingFile
}

// Return the number of pages in the heap file
func (f *HeapFile) NumPages() int {
	// TODO: some code goes here
	file, err := os.Open(f.backingFile)
	if err != nil {
		return 0
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return 0
	}
	return int(stat.Size()) / PageSize
}

// Load the contents of a heap file from a specified CSV file.  Parameters are as follows:
// - hasHeader:  whether or not the CSV file has a header
// - sep: the character to use to separate fields
// - skipLastField: if true, the final field is skipped (some TPC datasets include a trailing separator on each line)
// Returns an error if the field cannot be opened or if a line is malformed
// We provide the implementation of this method, but it won't work until
// [HeapFile.insertTuple] and some other utility functions are implemented
func (f *HeapFile) LoadFromCSV(file *os.File, hasHeader bool, sep string, skipLastField bool) error {
	scanner := bufio.NewScanner(file)
	cnt := 0
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Split(line, sep)
		if skipLastField {
			fields = fields[0 : len(fields)-1]
		}
		numFields := len(fields)
		cnt++
		desc := f.Descriptor()
		if desc == nil || desc.Fields == nil {
			return GoDBError{MalformedDataError, "Descriptor was nil"}
		}
		if numFields != len(desc.Fields) {
			return GoDBError{MalformedDataError, fmt.Sprintf("LoadFromCSV:  line %d (%s) does not have expected number of fields (expected %d, got %d)", cnt, line, len(f.Descriptor().Fields), numFields)}
		}
		if cnt == 1 && hasHeader {
			continue
		}
		var newFields []DBValue
		for fno, field := range fields {
			switch f.Descriptor().Fields[fno].Ftype {
			case IntType:
				field = strings.TrimSpace(field)
				floatVal, err := strconv.ParseFloat(field, 64)
				if err != nil {
					return GoDBError{TypeMismatchError, fmt.Sprintf("LoadFromCSV: couldn't convert value %s to int, tuple %d", field, cnt)}
				}
				intValue := int(floatVal)
				newFields = append(newFields, IntField{int64(intValue)})
			case StringType:
				if len(field) > StringLength {
					field = field[0:StringLength]
				}
				newFields = append(newFields, StringField{field})
			}
		}
		newT := Tuple{*f.Descriptor(), newFields, nil}
		tid := NewTID()
		bp := f.bufPool
		f.insertTuple(&newT, tid)

		// Force dirty pages to disk. CommitTransaction may not be implemented
		// yet if this is called in lab 1 or 2.
		// bp.FlushAllPages()
		bp.CommitTransaction(tid)

	}
	return nil
}

// Read the specified page number from the HeapFile on disk. This method is
// called by the [BufferPool.GetPage] method when it cannot find the page in its
// cache.
//
// This method will need to open the file supplied to the constructor, seek to
// the appropriate offset, read the bytes in, and construct a [heapPage] object,
// using the [heapPage.initFromBuffer] method.
func (f *HeapFile) readPage(pageNo int) (Page, error) {
	// TODO: some code goes here
	file, err := os.Open(f.backingFile)

	if err != nil {
		return nil, err
	}
	defer file.Close()
	pageByte := make([]byte, PageSize)
	_, err = file.ReadAt(pageByte, int64(pageNo * PageSize))
	if err != nil {
		return nil, err
	}
	page, err := newHeapPage(f.desc, pageNo, f)
	if err != nil {
		return nil, err
	}
	if err := page.initFromBuffer(bytes.NewBuffer(pageByte)); err != nil {
		return nil, err
	}
	return page, nil
}

// Add the tuple to the HeapFile. This method should search through pages in the
// heap file, looking for empty slots and adding the tuple in the first empty
// slot if finds.
//
// If none are found, it should create a new [heapPage] and insert the tuple
// there, and write the heapPage to the end of the HeapFile (e.g., using the
// [flushPage] method.)
//
// To iterate through pages, it should use the [BufferPool.GetPage method]
// rather than directly reading pages itself. For lab 1, you do not need to
// worry about concurrent transactions modifying the Page or HeapFile. We will
// add support for concurrent modifications in lab 3.
//
// The page the tuple is inserted into should be marked as dirty.
func (f *HeapFile) insertTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	numPages := f.NumPages()
	for pageNo := 0; pageNo < numPages; pageNo++ {
		page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
		if err != nil {
			return err
		}
		if _, err := page.(*heapPage).insertTupleAndSetDirty(t, tid); err != nil {
			if err.(GoDBError).code == PageFullError {
				continue
			}
			return err
		} else {
			return nil
		}
	}
	// prevent two transactions from inserting a new tuple that adds a new page to the HeapFile
	f.mu.Lock()
	numPages = f.NumPages()
	page, err := newHeapPage(f.desc, numPages, f)

	if err != nil {
		f.mu.Unlock()
		return err
	}
	// flush empty page
	if err := f.flushPage(page); err != nil {
		f.mu.Unlock()
		return err
	}
	f.mu.Unlock()
	// then reobtain the page from buffer pool & insert tuple
	newPage, err := f.bufPool.GetPage(f, numPages, tid, WritePerm)
	if err != nil {
		return err
	}
	if _, err := newPage.(*heapPage).insertTupleAndSetDirty(t, tid); err != nil {
		return err
	}
	return nil
}

// Remove the provided tuple from the HeapFile.
//
// This method should use the [Tuple.Rid] field of t to determine which tuple to
// remove. The Rid field should be set when the tuple is read using the
// [Iterator] method, or is otherwise created (as in tests). Note that Rid is an
// empty interface, so you can supply any object you wish. You will likely want
// to identify the heap page and slot within the page that the tuple came from.
//
// The page the tuple is deleted from should be marked as dirty.
func (f *HeapFile) deleteTuple(t *Tuple, tid TransactionID) error {
	// TODO: some code goes here
	pageNo := t.Rid.(rID).pageNo
	page, err := f.bufPool.GetPage(f, pageNo, tid, WritePerm)
	if err != nil {
		return err
	}
	return page.(*heapPage).deleteTupleAndSetDirty(t.Rid, tid)
}

// Method to force the specified page back to the backing file at the
// appropriate location. This will be called by BufferPool when it wants to
// evict a page. The Page object should store information about its offset on
// disk (e.g., that it is the ith page in the heap file), so you can determine
// where to write it back.
func (f *HeapFile) flushPage(p Page) error {
	// TODO: some code goes here
	page := p.(*heapPage)
	buf, err := page.toBuffer()
	if err != nil {
		return err
	}
	file, err := os.OpenFile(f.backingFile, os.O_WRONLY, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteAt(buf.Bytes(), int64(page.pageNo * PageSize))
	return err
}

// [Operator] descriptor method -- return the TupleDesc for this HeapFile
// Supplied as argument to NewHeapFile.
func (f *HeapFile) Descriptor() *TupleDesc {
	// TODO: some code goes here
	return f.desc

}

// [Operator] iterator method
// Return a function that iterates through the records in the heap file
// Note that this method should read pages from the HeapFile using the
// BufferPool method GetPage, rather than reading pages directly,
// since the BufferPool caches pages and manages page-level locking state for
// transactions
// You should esnure that Tuples returned by this method have their Rid object
// set appropriate so that [deleteTuple] will work (see additional comments there).
// Make sure to set the returned tuple's TupleDescriptor to the TupleDescriptor of
// the HeapFile. This allows it to correctly capture the table qualifier.
func (f *HeapFile) Iterator(tid TransactionID) (func() ([]*Tuple, error), error) {
	// TODO: some code goes here
	numPages := f.NumPages()
	pageNo := -1
	var iter func() ([]*Tuple, error)

	goToNextPage := func() (func() ([]*Tuple, error), error) {
		pageNo++
		if pageNo >= numPages {
			return nil, nil
		}
		page, err := f.bufPool.GetPage(f, pageNo, tid, ReadPerm)
		if err != nil {
			return nil, err
		}
		return page.(*heapPage).tupleIter(), nil
	}
	iter, err := goToNextPage()
	var currBatch []*Tuple
	if err != nil {
		return nil, err
	}
	
	return func() ([]*Tuple, error) {
		var returnBatch []*Tuple
		for iter != nil {
			batch, err := iter()
			if err != nil {
				return nil, err
			}
			if len(batch) == 0 {
				iter, err = goToNextPage()
				if err != nil {
					return nil, err
				}
				continue
			}
			currBatch = append(currBatch, batch...)
			if len(currBatch) >= BatchSize {
				returnBatch = currBatch[:BatchSize]
				currBatch = currBatch[BatchSize:]
				break
			}
		}
		if len(returnBatch) == 0 {
			returnBatch = currBatch
			currBatch = nil
		}
		for _, t := range returnBatch {
			t.Desc = *f.desc
		}
		return returnBatch, nil
	}, nil
}

// internal strucuture to use as key for a heap page
type heapHash struct {
	FileName string
	PageNo   int
}

// This method returns a key for a page to use in a map object, used by
// BufferPool to determine if a page is cached or not.  We recommend using a
// heapHash struct as the key for a page, although you can use any struct that
// does not contain a slice or a map that uniquely identifies the page.
func (f *HeapFile) pageKey(pgNo int) any {
	// TODO: some code goes here
	return heapHash{f.backingFile, pgNo}
}
