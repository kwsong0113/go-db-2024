package godb

import (
	"sync"
	"time"
)

//BufferPool provides methods to cache pages that have been read from disk.
//It has a fixed capacity to limit the total amount of memory used by GoDB.
//It is also the primary way in which transactions are enforced, by using page
//level locking (you will not need to worry about this until lab3).

// Permissions used to when reading / locking pages
type RWPerm int

const (
	ReadPerm  RWPerm = iota
	WritePerm RWPerm = iota
)

type PageLock struct {
	sharedTids map[TransactionID]bool
	exclusiveTid *TransactionID
}

type BufferPool struct {
	// TODO: some code goes here
	runningTids map[TransactionID]bool
	pages map[any]Page
	pageLocks map[any]*PageLock
	numPages int
	waitFor map[TransactionID]map[TransactionID]bool
	mu sync.Mutex
}

// Create a new BufferPool with the specified number of pages
func NewBufferPool(numPages int) (*BufferPool, error) {
	return &BufferPool{
		runningTids: make(map[TransactionID]bool),
		pages: make(map[any]Page),
		pageLocks: make(map[any]*PageLock),
		numPages: numPages,
		waitFor: make(map[TransactionID]map[TransactionID]bool),
	}, nil
}

// Testing method -- iterate through all pages in the buffer pool
// and flush them using [DBFile.flushPage]. Does not need to be thread/transaction safe.
// Mark pages as not dirty after flushing them.
func (bp *BufferPool) FlushAllPages() {
	// TODO: some code goes here
	if bp == nil {
		return
	}
	for _, page := range bp.pages {
		page.getFile().flushPage(page)
		page.setDirty(0, false)
	}
}

// Abort the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk so it is sufficient to just
// release locks to abort. You do not need to implement this for lab 1.
func (bp *BufferPool) AbortTransaction(tid TransactionID) {
	// TODO: some code goes here
	bp.mu.Lock()
	defer bp.mu.Unlock()
	// revert dirty pages associated with the transaction
	for pageKey, page := range bp.pages {
		if page.isDirty() && *page.(*heapPage).dirtyBy == tid {
			delete(bp.pages, pageKey)
		}
	}
	// release locks
	bp.releaseLocks(tid)
	// clean up the wait-for graph
	bp.clearWaitFor(tid)
	// delete transaction from running transactions
	delete(bp.runningTids, tid)
}

// Commit the transaction, releasing locks. Because GoDB is FORCE/NO STEAL, none
// of the pages tid has dirtied will be on disk, so prior to releasing locks you
// should iterate through pages and write them to disk.  In GoDB lab3 we assume
// that the system will not crash while doing this, allowing us to avoid using a
// WAL. You do not need to implement this for lab 1.
func (bp *BufferPool) CommitTransaction(tid TransactionID) {
	// TODO: some code goes here
	bp.mu.Lock()
	defer bp.mu.Unlock()
	// flush dirty pages associated with the transaction
	for _, page := range bp.pages {
		if page.isDirty() && *page.(*heapPage).dirtyBy == tid {
			page.getFile().flushPage(page)
			page.setDirty(0, false)
		}
	}
	// release locks
	bp.releaseLocks(tid)
	// clean up the wait-for graph
	bp.clearWaitFor(tid)
	// delete transaction from running transactions
	delete(bp.runningTids, tid)
}

// Release locks held by the transaction
func (bp *BufferPool) releaseLocks(tid TransactionID) {
	for key, pageLock := range bp.pageLocks {
		if pageLock.sharedTids[tid] {
			delete(pageLock.sharedTids, tid)
		} else if pageLock.exclusiveTid != nil && *pageLock.exclusiveTid == tid {
			delete(bp.pageLocks, key)
		}
	}
}

// Clean up the wait-for graph for the transaction
func (bp *BufferPool) clearWaitFor(tid TransactionID) {
	for source := range bp.waitFor {
		delete(bp.waitFor[source], tid)
	}
	delete(bp.waitFor, tid)
}

// Begin a new transaction. You do not need to implement this for lab 1.
//
// Returns an error if the transaction is already running.
func (bp *BufferPool) BeginTransaction(tid TransactionID) error {
	// TODO: some code goes here
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if _, ok := bp.runningTids[tid]; ok {
		return GoDBError{
			code: IllegalTransactionError,
			errString: "transaction is already running",
		}
	}
	bp.runningTids[tid] = true
	return nil
}

// Update the wait-for graph and detect deadlocks.
// Returns true if introducing new dependencies would create a deadlock.
func (bp *BufferPool) detectDeadlock(source TransactionID, targets map[TransactionID]bool) bool {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	// update wait-for graph
	bp.waitFor[source] = targets
	// detect deadlock using DFS
	visited := make(map[TransactionID]bool)
	currentPath := make(map[TransactionID]bool)
	// DFS function that returns true if a deadlock is detected
	var dfs func(TransactionID) bool
	dfs = func(tid TransactionID) bool {
		if currentPath[tid] {
			return true
		}
		if visited[tid] {
			return false
		}
		visited[tid] = true
		currentPath[tid] = true
		for target := range bp.waitFor[tid] {
			if dfs(target) {
				return true
			}
		}
		delete(currentPath, tid)
		return false
	}
	// run DFS
	for tid := range bp.waitFor {
		if !visited[tid] {
			if dfs(tid) {
				return true
			}
		}
	}
	return false
}

// Retrieve the specified page from the specified DBFile (e.g., a HeapFile), on
// behalf of the specified transaction. If a page is not cached in the buffer pool,
// you can read it from disk uing [DBFile.readPage]. If the buffer pool is full (i.e.,
// already stores numPages pages), a page should be evicted.  Should not evict
// pages that are dirty, as this would violate NO STEAL. If the buffer pool is
// full of dirty pages, you should return an error. Before returning the page,
// attempt to lock it with the specified permission.  If the lock is
// unavailable, should block until the lock is free. If a deadlock occurs, abort
// one of the transactions in the deadlock. For lab 1, you do not need to
// implement locking or deadlock detection. You will likely want to store a list
// of pages in the BufferPool in a map keyed by the [DBFile.pageKey].
func (bp *BufferPool) GetPage(file DBFile, pageNo int, tid TransactionID, perm RWPerm) (Page, error) {
	pageKey := file.pageKey(pageNo)
	// acquire lock
	for {
		bp.mu.Lock()
		if _, ok := bp.pageLocks[pageKey]; !ok {
			bp.pageLocks[pageKey] = &PageLock{
				sharedTids: make(map[TransactionID]bool),
				exclusiveTid: nil,
			}
		}
		pageLock := bp.pageLocks[pageKey]
		acquired := false
		waitForTargets := make(map[TransactionID]bool)
		// first check if the transaction already has the lock
		if pageLock.sharedTids[tid] {
			if perm == ReadPerm {
				acquired = true
			} else {
				// upgrade to exclusive lock
				if len(pageLock.sharedTids) == 1 {
					delete(pageLock.sharedTids, tid)
					pageLock.exclusiveTid = &tid
					acquired = true
				} else {
					for t := range pageLock.sharedTids {
						if t != tid {
							waitForTargets[t] = true
						}
					}
				}
			}
		} else if pageLock.exclusiveTid != nil && *pageLock.exclusiveTid == tid {
			acquired = true
		} else if perm == ReadPerm {
		// now check if the page can acquire a new lock
			if pageLock.exclusiveTid == nil {
				pageLock.sharedTids[tid] = true
				acquired = true
			} else {
				waitForTargets[*pageLock.exclusiveTid] = true
			}
		} else {
			if len(pageLock.sharedTids) == 0 && pageLock.exclusiveTid == nil {
				pageLock.exclusiveTid = &tid
				acquired = true
			} else {
				for t := range pageLock.sharedTids {
					waitForTargets[t] = true
				}
				if pageLock.exclusiveTid != nil {
					waitForTargets[*pageLock.exclusiveTid] = true
				}
			}
		}
		bp.mu.Unlock()
		if acquired {
			break // lock acquired, so proceed
		} else {
			// deadlock detection
			if bp.detectDeadlock(tid, waitForTargets) {
				// abort the transaction
				bp.AbortTransaction(tid)
				return nil, GoDBError{
					code: DeadlockError,
					errString: "deadlock detected",
				}
			}
			time.Sleep(10 * time.Millisecond) // block
		}
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()
	if page, ok := bp.pages[pageKey]; ok {
		return page, nil
	}
	page, err := file.readPage(pageNo)
	if err != nil {
		return nil, err
	}
	if len(bp.pages) == bp.numPages {
		deleted := false
		// if full, evict a page with random eviction policy
		for key, page := range bp.pages {
			if !page.isDirty() {
				delete(bp.pages, key)
				deleted = true
				break
			}
		}
		if !deleted {
			return nil, GoDBError{
				code: BufferPoolFullError,
				errString: "buffer pool is full",
			}
		}
	}
	bp.pages[pageKey] = page
	return page, nil
}
