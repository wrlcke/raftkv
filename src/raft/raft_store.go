package raft

import (
	"bytes"
	"sync"

	"6.5840/labgob"
)

// RaftStore is a not thread safe interface
// But Meta and Logs are considered seperate, so you can access two parts concurrently
// Cache and Storage are also considered seperate
// You can access meta and cache logs in one goroutine and storage logs in another goroutine without a lock
// but don't access meta in two goroutines or access storage logs in two goroutines unless you use a lock
type RaftStore interface {
	CurrentTerm() int
	VotedFor() int
	SetCurrentTerm(term int)
	SetVotedFor(votedFor int)
	SetCurrentTermAndVotedFor(term int, votedFor int)

	FirstLogIndex() int                        // First log index in log array
	LastLogIndex() int                        // Last log index in log array
	LastLogTerm() int                         // Last log term in log array
	Log(index int) *LogEntry                  // Get log at index
	Logs(start int, end int) []LogEntry       // Get logs from start (inclusive) to end (exclusive) 
	AppendLogs(logs ...LogEntry)              // Append logs to log array
	AmendLogs(suffix int, logs ...LogEntry)   // Remove logs after suffix (inclusive) and append logs from suffix (inclusive)
	TrimLogs(prefix int)                      // Remove logs before prefix (exlusive)
	TrimAndAmendLogs(prefix int, suffix int, logs ...LogEntry) 
	                                          // Remove logs before prefix (exlusive) and after suffix (inclusive) and append logs from suffix (inclusive)
	CopyLogEntries(start int, end int) []LogEntry

	Cache() RaftStore                       // Offered to manipulate in memory cache of raft state
	Storage() RaftStore                     // Offered to manipulate persistent storage of raft state
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
	return ms.log[start - ms.firstLogIndex : end - ms.firstLogIndex]
}

func (ms *MemoryRaftStore) AppendLogs(logs ...LogEntry) {
	ms.log = append(ms.log, logs...)
}

func (ms *MemoryRaftStore) TrimLogs(prefix int) {
	ms.log = ms.log[prefix - ms.firstLogIndex :]
	ms.firstLogIndex = prefix
}

func (ms *MemoryRaftStore) AmendLogs(suffix int, logs ...LogEntry) {
	ms.log = append(ms.log[:suffix - ms.firstLogIndex], logs...)
}

func (ms *MemoryRaftStore) TrimAndAmendLogs(prefix int, suffix int, logs ...LogEntry) {
	ms.log = append(ms.log[prefix - ms.firstLogIndex : suffix - ms.firstLogIndex], logs...)
	ms.firstLogIndex = prefix
}

func (ms *MemoryRaftStore) CopyLogEntries(start int, end int) []LogEntry {
	dst := make([]LogEntry, end - start)
	copy(dst, ms.Logs(start, end))
	return dst
}

func (ms *MemoryRaftStore) Cache() RaftStore {
	return ms
}

func (ms *MemoryRaftStore) Storage() RaftStore {
	return ms
}

type LabPersister struct {
	MemoryRaftStore
	storage LabPersisterStorage
}

type LabPersisterStorage struct {
	MemoryRaftStore
	rw sync.RWMutex              // because the persister provided by the lab mixes all states together and must be persisted together, use read write lock to protect currentTerm or votedFor when performing async log write
	persister *Persister
}

func MakeLabPersister(persister *Persister) *LabPersister {
	lp := &LabPersister{storage: LabPersisterStorage{persister: persister}}
	lp.storage.readPersist(persister.ReadRaftState())
	cachelog := make([]LogEntry, len(lp.storage.log))
	copy(cachelog, lp.storage.log)
	lp.currentTerm, lp.votedFor, lp.firstLogIndex, lp.log =
		lp.storage.currentTerm, lp.storage.votedFor, lp.storage.firstLogIndex, cachelog
	return lp
}

func (lp *LabPersister) Cache() RaftStore {
	return &lp.MemoryRaftStore
}

func (lp *LabPersister) Storage() RaftStore {
	return &lp.storage
}

func (lp *LabPersister) SetCurrentTerm(term int) {
	lp.MemoryRaftStore.SetCurrentTerm(term)
	lp.storage.SetCurrentTerm(term)
}

func (lp *LabPersister) SetVotedFor(votedFor int) {
	lp.MemoryRaftStore.SetVotedFor(votedFor)
	lp.storage.SetVotedFor(votedFor)
}

func (lp *LabPersister) SetCurrentTermAndVotedFor(term int, votedFor int) {
	lp.MemoryRaftStore.SetCurrentTermAndVotedFor(term, votedFor)
	lp.storage.SetCurrentTermAndVotedFor(term, votedFor)
}

func (lp *LabPersister) AppendLogs(logs ...LogEntry) {
	lp.MemoryRaftStore.AppendLogs(logs...)
	lp.storage.AppendLogs(logs...)
}

func (lp *LabPersister) AmendLogs(suffix int, logs ...LogEntry) {
	lp.MemoryRaftStore.AmendLogs(suffix, logs...)
	lp.storage.AmendLogs(suffix, logs...)
}

func (lp *LabPersister) TrimLogs(prefix int) {
	lp.MemoryRaftStore.TrimLogs(prefix)
	lp.storage.TrimLogs(prefix)
}

func (lp *LabPersister) TrimAndAmendLogs(prefix int, suffix int, logs ...LogEntry) {
	lp.MemoryRaftStore.TrimAndAmendLogs(prefix, suffix, logs...)
	lp.storage.TrimAndAmendLogs(prefix, suffix, logs...)
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
	t, v, f, l := lps.currentTerm, lps.votedFor, lps.firstLogIndex, make([]LogEntry, len(lps.log))
	copy(l, lps.log)
	lps.rw.RUnlock()
	e.Encode(t)
	e.Encode(v)
	e.Encode(f)
	e.Encode(l)
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
}

func (lps *LabPersisterStorage) CurrentTerm() int {
	lps.rw.RLock()
	defer lps.rw.RUnlock()
	return lps.currentTerm
}

func (lps *LabPersisterStorage) VotedFor() int {
	lps.rw.RLock()
	defer lps.rw.RUnlock()
	return lps.votedFor
}

func (lps *LabPersisterStorage) SetCurrentTerm(term int) {
	lps.rw.Lock()
	lps.currentTerm = term
	lps.rw.Unlock()
	lps.persist()
}

func (lps *LabPersisterStorage) SetVotedFor(votedFor int) {
	lps.rw.Lock()
	lps.votedFor = votedFor
	lps.rw.Unlock()
	lps.persist()
}

func (lps *LabPersisterStorage) SetCurrentTermAndVotedFor(term int, votedFor int) {
	lps.rw.Lock()
	lps.currentTerm, lps.votedFor = term, votedFor
	lps.rw.Unlock()
	lps.persist()
}

func (lps *LabPersisterStorage) FirstLogIndex() int {
	lps.rw.RLock()
	defer lps.rw.RUnlock()
	return lps.firstLogIndex
}

func (lps *LabPersisterStorage) LastLogIndex() int {
	lps.rw.RLock()
	defer lps.rw.RUnlock()
	return len(lps.log) - 1 + lps.firstLogIndex
}

func (lps *LabPersisterStorage) LastLogTerm() int {
	lps.rw.RLock()
	defer lps.rw.RUnlock()
	return lps.log[len(lps.log) - 1].Term
}

func (lps *LabPersisterStorage) Log(index int) *LogEntry {
	lps.rw.RLock()
	defer lps.rw.RUnlock()
	return &lps.log[index - lps.firstLogIndex]
}

func (lps *LabPersisterStorage) Logs(start int, end int) []LogEntry {
	lps.rw.RLock()
	defer lps.rw.RUnlock()
	return lps.log[start - lps.firstLogIndex : end - lps.firstLogIndex]
}

func (lps *LabPersisterStorage) AppendLogs(logs ...LogEntry) {
	lps.rw.Lock()
	lps.log = append(lps.log, logs...)
	lps.rw.Unlock()
	lps.persist()
}

func (lps *LabPersisterStorage) TrimLogs(prefix int) {
	lps.rw.Lock()
	lps.log = lps.log[prefix - lps.firstLogIndex :]
	lps.firstLogIndex = prefix
	lps.rw.Unlock()
	lps.persist()
}

func (lps *LabPersisterStorage) AmendLogs(suffix int, logs ...LogEntry) {
	lps.rw.Lock()
	lps.log = append(lps.log[:suffix - lps.firstLogIndex], logs...)
	lps.rw.Unlock()
	lps.persist()
}

func (lps *LabPersisterStorage) TrimAndAmendLogs(prefix int, suffix int, logs ...LogEntry) {
	lps.rw.Lock()
	lps.log = append(lps.log[prefix - lps.firstLogIndex : suffix - lps.firstLogIndex], logs...)
	lps.firstLogIndex = prefix
	lps.rw.Unlock()
	lps.persist()
}

func (lps *LabPersisterStorage) CopyLogEntries(start int, end int) []LogEntry {
	dst := make([]LogEntry, end - start)
	lps.rw.RLock()
	copy(dst, lps.Logs(start, end))
	lps.rw.RUnlock()
	return dst
}

func (lps *LabPersisterStorage) Cache() RaftStore {
	return lps
}

func (lps *LabPersisterStorage) Storage() RaftStore {
	return lps
}
