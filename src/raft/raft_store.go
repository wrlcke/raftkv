package raft

import (
	"bytes"
	"sync"

	"6.5840/labgob"
)

// All methods except FlushLogs() are manipulating in-memory data and should be called in the same goroutine or protected by a lock
// FlushLogs() will persist all new logs since last call of FlushLogs, and allow calling in a different goroutine than the other functions are called (asynchronous persist)
// But FlushLogs() itself is not thread safe, so it should be called in the same goroutine 
type RaftStore interface {
	CurrentTerm() int
	VotedFor() int
	SetCurrentTerm(term int)
	SetVotedFor(votedFor int)
	SetCurrentTermAndVotedFor(term int, votedFor int)

	FirstLogIndex() int                       // First log index in log array
	LastLogIndex() int                        // Last log index in log array
	LogTerm(index int) int                    // Get log term at index
	Log(index int) *LogEntry                  // Get log at index
	Logs(start int, end int) []LogEntry       // Get a copy of logs from start (inclusive) to end (exclusive) 
	AppendLogs(logs ...LogEntry)              // Append logs to log array
	RemoveLogPrefix(end int)                  // Remove logs before end (exlusive), and end will be preserved with corresponding term but no command
	RemoveLogSuffix(start int)                // Remove logs after start (inclusive)
	ResetLog(lastIndex int, lastTerm int)     // Reset log array to contain only one entry with given index and term, used when a snapshot is installed from leader

	FlushLogs()  (int, int)                   // Flush logs to storage

	SetSnapshot(snapshot Snapshot)            // Set snapshot (make sure the data will not be modified after calling this function)
	Snapshot() Snapshot					      // Get snapshot (make sure the return value only be read)
}

type Snapshot []byte

type MemoryRaftStore struct {
	currentTerm int
	votedFor int
	startLogIndex int             // startLogIndex is the global index of log[0] after a snapshot, representing the last log entry in the snapshot. Its term is used for matching purposes when sending RPCs, and log[1] onwards are the actual logs, log[1]'s index will be the FirstLogIndex()
	log []LogEntry
	snapshot []byte
}

func (ms *MemoryRaftStore) CurrentTerm() int {
	return ms.currentTerm
}

func (ms *MemoryRaftStore) VotedFor() int {
	return ms.votedFor
}

func (ms *MemoryRaftStore) SetCurrentTerm(term int) {
	ms.currentTerm = term
}

func (ms *MemoryRaftStore) SetVotedFor(votedFor int) {
	ms.votedFor = votedFor
}

func (ms *MemoryRaftStore) SetCurrentTermAndVotedFor(term int, votedFor int) {
	ms.currentTerm = term
	ms.votedFor = votedFor
}

func (ms *MemoryRaftStore) FirstLogIndex() int {
	return ms.startLogIndex + 1
}

func (ms *MemoryRaftStore) LastLogIndex() int {
	return len(ms.log) - 1 + ms.startLogIndex
}

func (ms *MemoryRaftStore) LogTerm(index int) int {
	return ms.log[index - ms.startLogIndex].Term
}

func (ms *MemoryRaftStore) Log(index int) *LogEntry {
	return &ms.log[index - ms.startLogIndex]
}

func (ms *MemoryRaftStore) Logs(start int, end int) []LogEntry {
	dst := make([]LogEntry, end - start)
	copy(dst, ms.log[start - ms.startLogIndex : end - ms.startLogIndex])
	return dst
}

func (ms *MemoryRaftStore) AppendLogs(logs ...LogEntry) {
	ms.log = append(ms.log, logs...)
}

func (ms *MemoryRaftStore) RemoveLogPrefix(end int) {
	copy(ms.log, ms.log[end - ms.startLogIndex:])
	ms.log = ms.log[:len(ms.log) - end + ms.startLogIndex]
	ms.log[0].Command = nil
	ms.startLogIndex = end
}

func (ms *MemoryRaftStore) RemoveLogSuffix(start int) {
	ms.log = ms.log[:start - ms.startLogIndex]
}

func (ms *MemoryRaftStore) ResetLog(lastIndex int, lastTerm int) {
	ms.startLogIndex = lastIndex
	ms.log[0] = LogEntry{lastTerm, nil}
	ms.log = ms.log[:1]
}

func (ms *MemoryRaftStore) SetSnapshot(snapshot Snapshot) {
	ms.snapshot = snapshot
}

func (ms *MemoryRaftStore) Snapshot() Snapshot {
	return ms.snapshot
}

type LabPersister struct {
	MemoryRaftStore
	buffer LabPersisterBuffer
	storage LabPersisterStorage
}

func MakeLabPersister(persister *Persister) *LabPersister {
	lp := &LabPersister{storage: LabPersisterStorage{persister: persister}}
	lp.storage.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	memlog := make([]LogEntry, len(lp.storage.log), cap(lp.storage.log))
	copy(memlog, lp.storage.log)
	lp.currentTerm, lp.votedFor, lp.startLogIndex, lp.log, lp.snapshot =
		lp.storage.currentTerm, lp.storage.votedFor, lp.storage.startLogIndex, memlog, lp.storage.snapshot
	lp.buffer.appendLogs = make([]LogEntry, 0, 2000)
	lp.buffer.appendIndex = lp.LastLogIndex() + 1
	lp.buffer.compactIndex = lp.startLogIndex
	lp.buffer.resetIndex, lp.buffer.resetTerm = lp.startLogIndex, lp.LogTerm(lp.startLogIndex)
	lp.buffer.snapshotUpdate, lp.buffer.snapshot = false, nil
	return lp
}

func (lp *LabPersister) SetCurrentTerm(term int) {
	lp.MemoryRaftStore.SetCurrentTerm(term)
	lp.storage.rw.Lock()            
	lp.storage.SetCurrentTerm(term)
	lp.storage.rw.Unlock()
	lp.storage.persist()
}

func (lp *LabPersister) SetVotedFor(votedFor int) {
	lp.MemoryRaftStore.SetVotedFor(votedFor)
	lp.storage.rw.Lock()
	lp.storage.SetVotedFor(votedFor)
	lp.storage.rw.Unlock()
	lp.storage.persist()
}

func (lp *LabPersister) SetCurrentTermAndVotedFor(term int, votedFor int) {
	lp.MemoryRaftStore.SetCurrentTermAndVotedFor(term, votedFor)
	lp.storage.rw.Lock()
	lp.storage.SetCurrentTermAndVotedFor(term, votedFor)
	lp.storage.rw.Unlock()
	lp.storage.persist()
}

func (lp *LabPersister) AppendLogs(logs ...LogEntry) {
	lp.MemoryRaftStore.AppendLogs(logs...)
	lp.buffer.rw.Lock()
	lp.buffer.appendLogs = append(lp.buffer.appendLogs, logs...)
	lp.buffer.clear = false
	lp.buffer.rw.Unlock()
}

func (lp *LabPersister) RemoveLogPrefix(end int) {
	lp.MemoryRaftStore.RemoveLogPrefix(end)
	lp.buffer.rw.Lock()
	lp.buffer.compactIndex = end
	lp.buffer.clear = false
	lp.buffer.rw.Unlock()
}

func (lp *LabPersister) RemoveLogSuffix(start int) {
	lp.MemoryRaftStore.RemoveLogSuffix(start)
	lp.buffer.rw.Lock()
	if start <= lp.buffer.appendIndex {
		lp.buffer.appendLogs = lp.buffer.appendLogs[:0]
		lp.buffer.appendIndex = start
	} else {
		lp.buffer.appendLogs = lp.buffer.appendLogs[:start - lp.buffer.appendIndex]
	}
	lp.buffer.clear = false
	lp.buffer.rw.Unlock()
}

func (lp *LabPersister) ResetLog(lastIndex int, lastTerm int) {
	lp.MemoryRaftStore.ResetLog(lastIndex, lastTerm)
	lp.buffer.rw.Lock()
	lp.buffer.resetIndex, lp.buffer.resetTerm = lastIndex, lastTerm
	lp.buffer.appendIndex = lastIndex + 1
	lp.buffer.appendLogs = lp.buffer.appendLogs[:0]
	lp.buffer.compactIndex = lastIndex
	lp.buffer.clear = false
	lp.buffer.rw.Unlock()
}

func (lp *LabPersister) SetSnapshot(snapshot Snapshot) {
	lp.MemoryRaftStore.SetSnapshot(snapshot)
	lp.buffer.rw.Lock()
	lp.buffer.snapshot = snapshot
	lp.buffer.snapshotUpdate = true
	lp.buffer.clear = false
	lp.buffer.rw.Unlock()
}

func (lp *LabPersister) FlushLogs() (int, int) {
	lp.buffer.rw.Lock()
	if lp.buffer.clear {
		lp.buffer.rw.Unlock()
		return lp.storage.FirstLogIndex(), lp.storage.LastLogIndex()
	}
	lp.storage.rw.Lock()
	// The buffer applying order here: reset -> append -> compact -> snapshot, is important
	if lp.buffer.resetIndex > lp.storage.startLogIndex {
		lp.storage.ResetLog(lp.buffer.resetIndex, lp.buffer.resetTerm)
	}
	if lp.buffer.appendIndex < lp.storage.LastLogIndex() + 1 {
		lp.storage.RemoveLogSuffix(lp.buffer.appendIndex)
	}
	if len(lp.buffer.appendLogs) > 0 {
		lp.storage.AppendLogs(lp.buffer.appendLogs...)
	}
	if lp.buffer.compactIndex > lp.storage.startLogIndex {
		lp.storage.RemoveLogPrefix(lp.buffer.compactIndex)
	}
	if lp.buffer.snapshotUpdate {
		lp.storage.SetSnapshot(lp.buffer.snapshot)
	}
	lp.storage.rw.Unlock()
	lp.buffer.appendLogs = lp.buffer.appendLogs[:0]
	lp.buffer.appendIndex = lp.storage.LastLogIndex() + 1
	lp.buffer.compactIndex = lp.storage.startLogIndex
	lp.buffer.resetIndex, lp.buffer.resetTerm = lp.storage.startLogIndex, lp.storage.LogTerm(lp.storage.startLogIndex)
	lp.buffer.snapshotUpdate, lp.buffer.snapshot = false, nil
	lp.buffer.clear = true
	lp.buffer.rw.Unlock()
	lp.storage.persist()
	return lp.storage.FirstLogIndex(), lp.storage.LastLogIndex()
}

type LabPersisterStorage struct {
	MemoryRaftStore
	rw sync.RWMutex            	  // because the persister provided by the lab mixes all states together and must be persisted together, use a read-write lock to protect the log and the other state
	persister *Persister
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (lps *LabPersisterStorage) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	lps.rw.RLock()
	defer lps.rw.RUnlock()
	e.Encode(lps.currentTerm)
	e.Encode(lps.votedFor)
	e.Encode(lps.startLogIndex)
	e.Encode(lps.log)
	raftstate := w.Bytes()
	lps.persister.Save(raftstate, lps.snapshot)
}

// restore previously persisted state.
func (lps *LabPersisterStorage) readPersist(raftstate []byte, snapshot []byte) {
	if raftstate == nil || len(raftstate) < 1 { // bootstrap without any state?
		lps.currentTerm = TermStart
		lps.votedFor = VotedForNone
		lps.startLogIndex = IndexStart
		lps.log = make([]LogEntry, 1, 10000)
		lps.log[0].Term, lps.log[0].Command = TermStart, nil
		lps.snapshot = nil
		lps.persist()
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(raftstate)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(raftstate)
	d := labgob.NewDecoder(r)
	lps.rw.Lock()
	defer lps.rw.Unlock()
	if d.Decode(&lps.currentTerm) != nil ||
		d.Decode(&lps.votedFor) != nil ||
		d.Decode(&lps.startLogIndex) != nil ||
		d.Decode(&lps.log) != nil {
		panic("Error reading persisted state")
	}
	lps.snapshot = snapshot
	// Expand capacity to reduce memory allocation for subsequent append operations
	lps.log = append(make([]LogEntry, 0, 10000), lps.log...) 
}

// The buffer track the unpersisted logs in memory 
type LabPersisterBuffer struct {
	appendIndex int
	appendLogs []LogEntry
	compactIndex int
	resetIndex int
	resetTerm int
	snapshotUpdate bool
	snapshot Snapshot
	clear bool
	rw sync.RWMutex
}
