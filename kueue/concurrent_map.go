package kueue

import (
	"hash/fnv"
	"sync"
)

// ConcurrentMapShard represents a single shard of the concurrent map.
// Fields are exported, so the user can directly access them if necessary.
type ConcurrentMapShard[K comparable, V any] struct {
	sync.RWMutex
	Items map[K]V
}

// ConcurrentMap is a thread-safe map sharded by key.
type ConcurrentMap[K comparable, V any] struct {
	shards     []*ConcurrentMapShard[K, V]
	shardCount int
}

// NewConcurrentMap creates a new ConcurrentMap with the given number of shards.
func NewConcurrentMap[K comparable, V any](shardCount int) *ConcurrentMap[K, V] {
	if shardCount <= 0 {
		shardCount = 16 // default to 16 shards if invalid shardCount is given
	}
	m := &ConcurrentMap[K, V]{
		shards:     make([]*ConcurrentMapShard[K, V], shardCount),
		shardCount: shardCount,
	}
	for i := 0; i < shardCount; i++ {
		m.shards[i] = &ConcurrentMapShard[K, V]{
			Items: make(map[K]V),
		}
	}
	return m
}

// hashKey returns the shard index for the given key.
// Currently assumes K is a string for hashing.
// For other key types, provide a custom hash function.
func (m *ConcurrentMap[K, V]) hashKey(key K) uint32 {
	strKey, ok := any(key).(string)
	if !ok {
		panic("This implementation requires K to be a string. For other types, provide a custom hash function.")
	}
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(strKey))
	return hasher.Sum32() % uint32(m.shardCount)
}

// ShardForKey returns the shard that would store the given key.
func (m *ConcurrentMap[K, V]) ShardForKey(key K) *ConcurrentMapShard[K, V] {
	return m.shards[m.hashKey(key)]
}

// ShardCount returns the number of shards.
func (m *ConcurrentMap[K, V]) ShardCount() int {
	return m.shardCount
}

// Shard returns the shard at the given index.
func (m *ConcurrentMap[K, V]) Shard(index int) *ConcurrentMapShard[K, V] {
	if index < 0 || index >= m.shardCount {
		panic("shard index out of range")
	}
	return m.shards[index]
}

// Set inserts or updates the value for a given key (convenience method).
func (m *ConcurrentMap[K, V]) Set(key K, value V) {
	shard := m.ShardForKey(key)
	shard.Lock()
	defer shard.Unlock()
	shard.Items[key] = value
}

// Get retrieves the value for a given key (convenience method). Returns (value, true) if found.
func (m *ConcurrentMap[K, V]) Get(key K) (V, bool) {
	shard := m.ShardForKey(key)
	shard.RLock()
	defer shard.RUnlock()
	val, ok := shard.Items[key]
	return val, ok
}

// Delete removes the key-value pair for a given key (convenience method).
func (m *ConcurrentMap[K, V]) Delete(key K) {
	shard := m.ShardForKey(key)
	shard.Lock()
	defer shard.Unlock()
	delete(shard.Items, key)
}

// Has checks if a key is present in the map (convenience method).
func (m *ConcurrentMap[K, V]) Has(key K) bool {
	shard := m.ShardForKey(key)
	shard.RLock()
	defer shard.RUnlock()
	_, ok := shard.Items[key]
	return ok
}

// Keys returns a slice containing all the keys in the map.
func (m *ConcurrentMap[K, V]) Keys() []K {
	keys := make([]K, 0)
	for _, shard := range m.shards {
		shard.RLock()
		for k := range shard.Items {
			keys = append(keys, k)
		}
		shard.RUnlock()
	}
	return keys
}

// Example usage
func main() {
	cm := NewConcurrentMap[string, string](4)

	// Convenience methods
	cm.Set("foo", "bar")
	val, ok := cm.Get("foo")
	if ok {
		println("foo:", val)
	}

	// Direct shard access
	shard := cm.ShardForKey("foo")
	shard.Lock()
	// Perform multiple operations atomically on this shard
	shard.Items["baz"] = "qux"
	delete(shard.Items, "foo")
	shard.Unlock()
}
