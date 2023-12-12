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
	LastLogTerm() int                         // Last log term in log array
	Log(index int) *LogEntry                  // Get log at index
	Logs(start int, end int) []LogEntry       // Get a copy of logs from start (inclusive) to end (exclusive) 
	AppendLogs(logs ...LogEntry)              // Append logs to log array
	RemoveLogPrefix(end int)                  // Remove logs before end (exlusive)
	RemoveLogSuffix(start int)                // Remove logs after start (inclusive)

	FlushLogs()  (int, int)                   // Flush logs to storage
}

type MemoryRaftStore struct {
	currentTerm int
	votedFor int
	firstLogIndex int
	log []LogEntry
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
	return ms.firstLogIndex
}

func (ms *MemoryRaftStore) LastLogIndex() int {
	return len(ms.log) - 1 + ms.firstLogIndex
}

func (ms *MemoryRaftStore) LastLogTerm() int {
	return ms.log[len(ms.log) - 1].Term
}

func (ms *MemoryRaftStore) Log(index int) *LogEntry {
	return &ms.log[index - ms.firstLogIndex]
}

func (ms *MemoryRaftStore) Logs(start int, end int) []LogEntry {
	dst := make([]LogEntry, end - start)
	copy(dst, ms.log[start - ms.firstLogIndex : end - ms.firstLogIndex])
	return dst
}

func (ms *MemoryRaftStore) AppendLogs(logs ...LogEntry) {
	ms.log = append(ms.log, logs...)
}

func (ms *MemoryRaftStore) RemoveLogPrefix(end int) {
	copy(ms.log, ms.log[end - ms.firstLogIndex:])
	ms.log = ms.log[:len(ms.log) - end + ms.firstLogIndex]
	ms.firstLogIndex = end
}

func (ms *MemoryRaftStore) RemoveLogSuffix(start int) {
	ms.log = ms.log[:start - ms.firstLogIndex]
}

type LabPersister struct {
	MemoryRaftStore
	buffer LabPersisterBuffer
	storage LabPersisterStorage
}

func MakeLabPersister(persister *Persister) *LabPersister {
	lp := &LabPersister{storage: LabPersisterStorage{persister: persister}}
	lp.storage.readPersist(persister.ReadRaftState())
	memlog := make([]LogEntry, len(lp.storage.log), cap(lp.storage.log))
	copy(memlog, lp.storage.log)
	lp.currentTerm, lp.votedFor, lp.firstLogIndex, lp.log =
		lp.storage.currentTerm, lp.storage.votedFor, lp.storage.firstLogIndex, memlog
	lp.buffer.appendLogs = make([]LogEntry, 0, 2000)
	lp.buffer.appendStartIndex = lp.LastLogIndex() + 1
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
	// TODO in Snapshot
	lp.buffer.clear = false
	lp.buffer.rw.Unlock()
}

func (lp *LabPersister) RemoveLogSuffix(start int) {
	lp.MemoryRaftStore.RemoveLogSuffix(start)
	lp.buffer.rw.Lock()
	if start <= lp.buffer.appendStartIndex {
		lp.buffer.appendLogs = lp.buffer.appendLogs[:0]
		lp.buffer.appendStartIndex = start
	} else {
		lp.buffer.appendLogs = lp.buffer.appendLogs[:start - lp.buffer.appendStartIndex]
	}
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
	lp.storage.log = append(lp.storage.log[:lp.buffer.appendStartIndex - lp.storage.firstLogIndex], lp.buffer.appendLogs...)
	lp.storage.rw.Unlock()
	lp.buffer.appendLogs = lp.buffer.appendLogs[:0]
	lp.buffer.appendStartIndex = lp.storage.LastLogIndex() + 1
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
	e.Encode(lps.firstLogIndex)
	e.Encode(lps.log)
	raftstate := w.Bytes()
	lps.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (lps *LabPersisterStorage) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		lps.currentTerm = TermStart
		lps.votedFor = VotedForNone
		lps.firstLogIndex = IndexStart
		lps.log = make([]LogEntry, IndexStart + 1, 10000)
		lps.persist()
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	lps.rw.Lock()
	defer lps.rw.Unlock()
	if d.Decode(&lps.currentTerm) != nil || d.Decode(&lps.votedFor) != nil || d.Decode(&lps.firstLogIndex) != nil || d.Decode(&lps.log) != nil {
		panic("Error reading persisted state")
	}
	// Expand capacity to reduce memory allocation for subsequent append operations
	lps.log = append(make([]LogEntry, 0, 10000), lps.log...) 
}

// The buffer track the unpersisted logs in memory 
type LabPersisterBuffer struct {
	appendStartIndex int
	appendLogs []LogEntry
	clear bool
	rw sync.RWMutex
}
