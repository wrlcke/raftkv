package kvraft

import (
	"bytes"

	"6.5840/labgob"
)

type KVStore interface {
	Put(key string, value string)
	Append(key string, value string)
	Get(key string) string
	Save() ([]byte, error)
	Load(data []byte) error
}

type MapKVStore struct {
	data map[string]string
}

func NewMapKVStore() *MapKVStore {
	return &MapKVStore{make(map[string]string)}
}

func (kv *MapKVStore) Put(key string, value string) {
	kv.data[key] = value
}

func (kv *MapKVStore) Append(key string, value string) {
	kv.data[key] += value
}

func (kv *MapKVStore) Get(key string) string {
	value, ok := kv.data[key]
	if ok {
		return value
	}
	return ""
}

func (kv *MapKVStore) Save() ([]byte, error) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.data)
	if err != nil {
		return nil, err
	}
	return w.Bytes(), err
}

func (kv *MapKVStore) Load(data []byte) error {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	return d.Decode(&kv.data)
}
