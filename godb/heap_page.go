package godb

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
	desc   *TupleDesc
	pageNo int
	file   *HeapFile
	dirtyBy *TransactionID
	tuples []*Tuple
	
}
type rID struct {
	pageNo int
	slotNo int
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) (*heapPage, error) {
	// TODO: some code goes here
	page := &heapPage{
		desc:   desc,
		pageNo: pageNo,
		file:   f,
		dirtyBy: nil,
	}
	page.tuples = make([]*Tuple, page.getNumSlots())
	return page, nil
}

func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
	return (PageSize - 8) / h.desc.ByteSize()
}

func (h *heapPage) getNumUsedSlots() int {
	count := 0
	for _, tuple := range h.tuples {
		if tuple != nil {
			count++
		}
	}
	return count
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here
	for slotNo, tuple := range h.tuples {
		if tuple == nil {
			t.Rid = h.slotNoToRid(slotNo)
			h.tuples[slotNo] = t
			return t.Rid, nil
		}
	}
	return "", GoDBError{code: PageFullError, errString: "no free slots on page"}
}

func (h *heapPage) insertTupleAndSetDirty(t *Tuple, tid TransactionID) (recordID, error) {
	rid, err := h.insertTuple(t)
	if err == nil {
		h.setDirty(tid, true)
	}
	return rid, err
}

// return a record ID for at the specified slot number
func (h *heapPage) slotNoToRid(slotNo int) recordID {
	return rID{pageNo: h.pageNo, slotNo: slotNo}
}

// return a slot number for the specified record ID
func (h *heapPage) ridToSlotNo(rid recordID) (int, error) {
	return rid.(rID).slotNo, nil
}


// Delete the tuple at the specified record ID, or return an error if the ID is
// invalid.
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO: some code goes here
	slotNo, err := h.ridToSlotNo(rid)
	if err != nil {
		return err
	}
	if h.tuples[slotNo] == nil {
		return GoDBError{code: TupleNotFoundError, errString: "tuple not found"}
	}
	h.tuples[slotNo] = nil
	return nil
}

func (h *heapPage) deleteTupleAndSetDirty(rid recordID, tid TransactionID) error {
	err := h.deleteTuple(rid)
	return err
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO: some code goes here
	return h.dirtyBy != nil
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(tid TransactionID, dirty bool) {
	// TODO: some code goes here
	if dirty {
		h.dirtyBy = &tid
	} else {
		h.dirtyBy = nil
	}
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() DBFile {
	// TODO: some code goes here
	return p.file
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO: some code goes here
	buf := new(bytes.Buffer)
	numSlots := int32(h.getNumSlots())
	numUsedSlots := int32(h.getNumUsedSlots())
	if err := binary.Write(buf, binary.LittleEndian, numSlots); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, numUsedSlots); err != nil {
		return nil, err
	}
	for _, tuple := range h.tuples {
		if tuple != nil {
			if err := tuple.writeTo(buf); err != nil {
				return nil, err
			}
		}
	}
	padding := make([]byte, PageSize-buf.Len())
	buf.Write(padding)
	return buf, nil
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// TODO: some code goes here
	var numSlots int32
	var numUsedSlots int32
	if err := binary.Read(buf, binary.LittleEndian, &numSlots); err != nil {
		return err
	}
	if numSlots != int32(h.getNumSlots()) {
		return GoDBError{
			code: MalformedDataError,
			errString: fmt.Sprintf("numSlots from buffer %d does not match expected %d", numSlots, h.getNumSlots()),
		}
	}
	if err := binary.Read(buf, binary.LittleEndian, &numUsedSlots); err != nil {
		return err
	}
	tuples := make([]*Tuple, numSlots)
	for slotNo := 0; slotNo < int(numUsedSlots); slotNo++ {
		if tuple, err := readTupleFrom(buf, h.desc); err != nil {
			return err
		} else {
			tuple.Rid = h.slotNoToRid(slotNo)
			tuples[slotNo] = tuple
		}
	}
	h.tuples = tuples
	return nil
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return an empty slice or nil if there are no tuples.
func (p *heapPage) tupleIter() func() ([]*Tuple, error) {
	slotNo := 0
	return func() ([]*Tuple, error) {
		var batch []*Tuple
		for len(batch) < BatchSize && slotNo < len(p.tuples) {
			if p.tuples[slotNo] != nil {
				batch = append(batch, p.tuples[slotNo])
			}
			slotNo++
		}

		return batch, nil
	}
}
