package kvraft

type KVStore interface {
	Put(key string, value string)
	Append(key string, value string)
	Get(key string) string
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
