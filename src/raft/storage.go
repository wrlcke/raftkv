package raft

import (
	"bytes"
	"sync"

	"6.5840/labgob"
)

type VolatileStorage interface {
	CurrentTerm() int
	VotedFor() int
	SetCurrentTerm(term int)
	SetVotedFor(votedFor int)

	FirstLogIndex() int                       // First log index in log array
	LastLogIndex() int                        // Last log index in log array
	LogTerm(index int) int                    // Get log term at index
	LogEntries(start int, end int) []LogEntry // Get a copy of log entries from start (inclusive) to end (exclusive)
	AppendLog(entries ...LogEntry)            // Append log entries to log array
	RemoveLogPrefix(end int)                  // Remove log entries before end (exlusive), and end will be preserved with corresponding term but no command
	RemoveLogSuffix(start int)                // Remove log entries after start (inclusive)
	ResetLog(lastIndex int, lastTerm int)     // Reset log array to contain only one entry with given index and term, used when a snapshot is installed from leader

	SetSnapshot(snapshot Snapshot) // Set snapshot (make sure the data will not be modified after calling this function)
	Snapshot() Snapshot            // Get snapshot (make sure the return value only be read)
}

type StableStorage interface {
	VolatileStorage
	Sync() // Sync all data to stable storage
}

type Storage interface {
	VolatileStorage
	Stable() StableStorage
}

type LabPersisterStorage struct {
	*MemoryStorage
	stable *LabPersisterStableStorage
}

func NewLabPersisterStorage(persister *Persister) *LabPersisterStorage {
	stable := &LabPersisterStableStorage{
		persister: persister,
	}
	stable.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	mem := &MemoryStorage{
		currentTerm:   stable.currentTerm,
		votedFor:      stable.votedFor,
		startLogIndex: stable.startLogIndex,
		log:           make([]LogEntry, len(stable.log), cap(stable.log)),
		snapshot:      stable.snapshot,
	}
	copy(mem.log, stable.log)
	return &LabPersisterStorage{MemoryStorage: mem, stable: stable}
}

func (s *LabPersisterStorage) Stable() StableStorage {
	return s.stable
}

type MemoryStorage struct {
	currentTerm   int
	votedFor      int
	startLogIndex int // startLogIndex is the global index of log[0] after a snapshot, representing the last log entry in the snapshot. Its term is used for matching purposes when sending RPCs, and log[1] onwards are the actual log, log[1]'s index will be the FirstLogIndex()
	log           []LogEntry
	snapshot      []byte
}

func (ms *MemoryStorage) CurrentTerm() int {
	return ms.currentTerm
}

func (ms *MemoryStorage) VotedFor() int {
	return ms.votedFor
}

func (ms *MemoryStorage) SetCurrentTerm(term int) {
	ms.currentTerm = term
}

func (ms *MemoryStorage) SetVotedFor(votedFor int) {
	ms.votedFor = votedFor
}

func (ms *MemoryStorage) FirstLogIndex() int {
	return ms.startLogIndex + 1
}

func (ms *MemoryStorage) LastLogIndex() int {
	return len(ms.log) - 1 + ms.startLogIndex
}

func (ms *MemoryStorage) LogTerm(index int) int {
	return ms.log[index-ms.startLogIndex].Term
}

func (ms *MemoryStorage) LogEntries(start int, end int) []LogEntry {
	dst := make([]LogEntry, end-start)
	copy(dst, ms.log[start-ms.startLogIndex:end-ms.startLogIndex])
	return dst
}

func (ms *MemoryStorage) AppendLog(entries ...LogEntry) {
	ms.log = append(ms.log, entries...)
}

func (ms *MemoryStorage) RemoveLogPrefix(end int) {
	copy(ms.log, ms.log[end-ms.startLogIndex:])
	ms.log = ms.log[:len(ms.log)-end+ms.startLogIndex]
	ms.log[0].Command = nil
	ms.startLogIndex = end
}

func (ms *MemoryStorage) RemoveLogSuffix(start int) {
	ms.log = ms.log[:start-ms.startLogIndex]
}

func (ms *MemoryStorage) ResetLog(lastIndex int, lastTerm int) {
	ms.startLogIndex = lastIndex
	ms.log[0] = LogEntry{lastTerm, nil}
	ms.log = ms.log[:1]
}

func (ms *MemoryStorage) SetSnapshot(snapshot Snapshot) {
	ms.snapshot = snapshot
}

func (ms *MemoryStorage) Snapshot() Snapshot {
	return ms.snapshot
}

type LabPersisterStableStorage struct {
	MemoryStorage
	rw        sync.RWMutex // because the persister provided by the lab mixes all states together and must be persisted together, use a read-write lock to protect the log and the other state
	persister *Persister
}

func (lps *LabPersisterStableStorage) SetCurrentTerm(term int) {
	lps.rw.Lock()
	defer lps.rw.Unlock()
	lps.MemoryStorage.SetCurrentTerm(term)
}

func (lps *LabPersisterStableStorage) SetVotedFor(votedFor int) {
	lps.rw.Lock()
	defer lps.rw.Unlock()
	lps.MemoryStorage.SetVotedFor(votedFor)
}

func (lps *LabPersisterStableStorage) AppendLog(entries ...LogEntry) {
	lps.rw.Lock()
	defer lps.rw.Unlock()
	lps.MemoryStorage.AppendLog(entries...)
}

func (lps *LabPersisterStableStorage) RemoveLogPrefix(end int) {
	lps.rw.Lock()
	defer lps.rw.Unlock()
	lps.MemoryStorage.RemoveLogPrefix(end)
}

func (lps *LabPersisterStableStorage) RemoveLogSuffix(start int) {
	lps.rw.Lock()
	defer lps.rw.Unlock()
	lps.MemoryStorage.RemoveLogSuffix(start)
}

func (lps *LabPersisterStableStorage) ResetLog(lastIndex int, lastTerm int) {
	lps.rw.Lock()
	defer lps.rw.Unlock()
	lps.MemoryStorage.ResetLog(lastIndex, lastTerm)
}

func (lps *LabPersisterStableStorage) SetSnapshot(snapshot Snapshot) {
	lps.rw.Lock()
	defer lps.rw.Unlock()
	lps.MemoryStorage.SetSnapshot(snapshot)
}

func (lps *LabPersisterStableStorage) Sync() {
	lps.persist()
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (lps *LabPersisterStableStorage) persist() {
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
func (lps *LabPersisterStableStorage) readPersist(raftstate []byte, snapshot []byte) {
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
