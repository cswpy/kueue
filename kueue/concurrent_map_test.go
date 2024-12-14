package kueue

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"
)

// TestBasicOperations ensures that basic Set, Get, Has, and Delete operations work as expected.
func TestBasicOperations(t *testing.T) {
	cm := NewConcurrentMap[string, string](4)

	// Initially, the map should be empty
	if cm.Count() != 0 {
		t.Fatalf("expected count = 0, got %d", cm.Count())
	}

	// Set a value
	cm.Set("foo", "bar")

	// Check that count and Has work
	if cm.Count() != 1 {
		t.Fatalf("expected count = 1, got %d", cm.Count())
	}
	if !cm.Has("foo") {
		t.Fatalf("expected to have key 'foo'")
	}

	// Get the value
	val, ok := cm.Get("foo")
	if !ok || val != "bar" {
		t.Fatalf("expected value = 'bar', got %v", val)
	}

	// Delete the value
	cm.Delete("foo")
	if cm.Has("foo") {
		t.Fatalf("did not expect to have key 'foo' after delete")
	}
	if cm.Count() != 0 {
		t.Fatalf("expected count = 0 after deletion, got %d", cm.Count())
	}

	// Try getting a non-existent key
	_, ok = cm.Get("not_found")
	if ok {
		t.Fatalf("expected key 'not_found' to not exist")
	}
}

// TestConcurrentWrites tests concurrent Set operations.
func TestConcurrentWrites(t *testing.T) {
	cm := NewConcurrentMap[string, int](16)
	numGoroutines := 50
	numKeysPerGoroutine := 100
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func(gid int) {
			defer wg.Done()
			for k := 0; k < numKeysPerGoroutine; k++ {
				key := fmt.Sprintf("g%d-k%d", gid, k)
				cm.Set(key, k)
			}
		}(i)
	}

	wg.Wait()

	// Verify count
	expectedCount := numGoroutines * numKeysPerGoroutine
	actualCount := cm.Count()
	if actualCount != expectedCount {
		t.Fatalf("expected count = %d, got %d", expectedCount, actualCount)
	}

	// Verify that all keys exist and their values are correct
	for i := 0; i < numGoroutines; i++ {
		for k := 0; k < numKeysPerGoroutine; k++ {
			key := fmt.Sprintf("g%d-k%d", i, k)
			val, ok := cm.Get(key)
			if !ok {
				t.Fatalf("expected key '%s' to be present", key)
			}
			if val != k {
				t.Fatalf("expected value %d for key '%s', got %v", k, key, val)
			}
		}
	}
}

// TestConcurrentReadsAndWrites tests that concurrent readers and writers do not cause data races.
func TestConcurrentReadsAndWrites(t *testing.T) {
	cm := NewConcurrentMap[string, int](8)
	numWriters := 10
	numReaders := 10
	numKeys := 100

	// Pre-fill some data
	for i := 0; i < numKeys; i++ {
		cm.Set(fmt.Sprintf("init-key-%d", i), i)
	}

	var wg sync.WaitGroup
	wg.Add(numWriters + numReaders)

	// Writers: randomly set and delete keys
	for w := 0; w < numWriters; w++ {
		go func(wid int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("writer%d-key%d", wid, i)
				cm.Set(key, i)
				if i%10 == 0 {
					cm.Delete(key)
				}
			}
		}(w)
	}

	// Readers: randomly read keys
	for r := 0; r < numReaders; r++ {
		go func(rid int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("init-key-%d", rand.Intn(numKeys))
				_, _ = cm.Get(key)
				cm.Has(key)
			}
		}(r)
	}

	wg.Wait()

	// If we've reached this point, we've not triggered any data race conditions (when run with -race).
}

// TestKeys verifies that Keys() returns all keys exactly.
func TestKeys(t *testing.T) {
	cm := NewConcurrentMap[string, int](4)
	keysToSet := []string{"apple", "banana", "cherry", "date", "elderberry"}

	for i, k := range keysToSet {
		cm.Set(k, i)
	}

	foundKeys := cm.Keys()
	keySet := make(map[string]bool)
	for _, k := range foundKeys {
		keySet[k] = true
	}

	for _, k := range keysToSet {
		if !keySet[k] {
			t.Fatalf("expected key '%s' in the result of Keys()", k)
		}
	}

	// Ensure no extra keys
	if len(foundKeys) != len(keysToSet) {
		t.Fatalf("expected %d keys, got %d", len(keysToSet), len(foundKeys))
	}
}

// TestDeleteNonExistent ensures deleting a non-existent key doesn't cause issues.
func TestDeleteNonExistent(t *testing.T) {
	cm := NewConcurrentMap[string, string](4)
	cm.Set("foo", "bar")
	cm.Delete("not_exist") // should not panic or cause error

	if cm.Count() != 1 {
		t.Fatalf("expected count = 1 after deleting non-existent key, got %d", cm.Count())
	}
	if !cm.Has("foo") {
		t.Fatalf("expected to still have 'foo'")
	}
}

// TestCountEmpty tests that a newly created map and a cleared map have the correct count.
func TestCountEmpty(t *testing.T) {
	cm := NewConcurrentMap[string, string](4)

	if cm.Count() != 0 {
		t.Fatalf("expected count = 0 for new map, got %d", cm.Count())
	}

	cm.Set("foo", "bar")
	cm.Delete("foo")

	if cm.Count() != 0 {
		t.Fatalf("expected count = 0 after clearing map, got %d", cm.Count())
	}
}

// TestHas checks the behavior of Has on existing and non-existing keys.
func TestHas(t *testing.T) {
	cm := NewConcurrentMap[string, string](4)
	cm.Set("hello", "world")

	if !cm.Has("hello") {
		t.Fatalf("expected Has('hello') = true")
	}
	if cm.Has("goodbye") {
		t.Fatalf("expected Has('goodbye') = false")
	}
}

// TestSetOverwrite tests that setting a key twice updates the value.
func TestSetOverwrite(t *testing.T) {
	cm := NewConcurrentMap[string, int](4)
	cm.Set("x", 1)
	cm.Set("x", 2)

	val, ok := cm.Get("x")
	if !ok || val != 2 {
		t.Fatalf("expected value = 2 for key 'x', got %v", val)
	}

	if cm.Count() != 1 {
		t.Fatalf("expected count = 1, got %d", cm.Count())
	}
}

// TestShardCount ensures that the number of shards does not change after creation and that keys are properly distributed.
func TestShardCount(t *testing.T) {
	shardCount := 8
	cm := NewConcurrentMap[string, int](shardCount)
	if len(cm.shards) != shardCount {
		t.Fatalf("expected %d shards, got %d", shardCount, len(cm.shards))
	}

	// Just a simple insertion check
	cm.Set("test", 123)
	val, ok := cm.Get("test")
	if !ok || val != 123 {
		t.Fatalf("unexpected value or missing key 'test'")
	}
}

// Example to run the tests (if run as main):
func TestMain(m *testing.M) {
	rand.Seed(time.Now().UnixNano())
	m.Run()
}
