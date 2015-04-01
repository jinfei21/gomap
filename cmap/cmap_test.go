package cmap

import (
	//"runtime"
	"strconv"
	//"sync"
	"testing"
)

func TestCHashMapPutAndGet(t *testing.T) {
	chm := NewConcurrentHashMapWithDefaultConfig()

	// put
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		chm.Put(key, i)
	}

	// test get
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		value := chm.Get(key)
		if i != value {
			t.Errorf("%d == %d", i, value)
		}
	}

	// test contains
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		if !chm.ContainsKey(key) {
			t.Errorf("not contain key %s", key)
		}
	}

	// put with value replacement
	for i := 0; i < 10240; i++ {
		key := strconv.Itoa(i)
		chm.Put(key, i+1)
	}

	// test get
	for i := 0; i < 10240; i++ {
		key := strconv.Itoa(i)
		value := chm.Get(key)
		if i+1 != value {
			t.Errorf("%d != %d", i+1, value)
		}
	}

	if chm.Size() != 10240 {
		t.Error("the size of hashmap is not 10240")
	}
}

func TestCHashMapPutIfAbsent(t *testing.T) {
	chm := NewConcurrentHashMapWithDefaultConfig()

	// put
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		if i%2 == 0 {
			old := chm.Put(key, i)
			if old != nil {
				t.Errorf("fail to put key %s which is aready present in the segment", key)
			}
		}
	}

	if chm.Size() != 512 {
		t.Error("the size of hashmap is not 1024")
	}

	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		if i%2 == 0 {
			old := chm.PutIfAbsent(key, i)
			if old != i {
				t.Errorf("existing value mismatch, %d != %d", old, i)
			}
		} else {
			old := chm.PutIfAbsent(key, i)
			if old != nil {
				t.Errorf("fail to put key %s which is aready present in the segment", key)
			}
		}
	}

	if chm.Size() != 1024 {
		t.Error("the size of hashmap is not 1024")
	}

}

func TestCHashMapSizeAndRemove(t *testing.T) {
	chm := NewConcurrentHashMapWithDefaultConfig()

	if chm.Size() != 0 && !chm.IsEmpty() {
		t.Error("hashmap is not empty")
	}

	// put
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		chm.Put(key, i)
	}

	if chm.IsEmpty() {
		t.Error("hashmap is empty")
	}
	if chm.Size() != 1024 {
		t.Errorf("hashmap size is not equal to %d", 1024)
	}

	// remove
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		value := chm.Remove(key)
		if i != value {
			t.Errorf("%d == %d", i, value)
		}
	}

	// test contains
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		if chm.ContainsKey(key) {
			t.Errorf("still contain key %s after removal", key)
		}
	}

	if chm.Size() != 0 && !chm.IsEmpty() {
		t.Error("hashmap is not empty after removal")
	}
}

func TestCHashMapRemoveIfValuePresent(t *testing.T) {
	chm := NewConcurrentHashMapWithDefaultConfig()

	// put
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		if i%2 == 0 {
			old := chm.Put(key, i)
			if old != nil {
				t.Errorf("fail to put key %s which is aready present in the segment", key)
			}
		} else {
			old := chm.Put(key, i+1)
			if old != nil {
				t.Errorf("fail to put key %s which is aready present in the segment", key)
			}
		}
	}

	// remove
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		if i%2 == 0 {
			removed := chm.RemoveIfPresent(key, i) // remove value present
			if !removed {
				t.Errorf("unable to remove key %s and value %d", key, i)
			}
		} else {
			removed := chm.RemoveIfPresent(key, i) // remove value absent
			if removed {
				t.Errorf("remove key %s should fail because of value mismatch", key)
			}
		}
	}
}

func TestCHashMapRepace(t *testing.T) {
	chm := NewConcurrentHashMapWithDefaultConfig()

	if chm.Size() != 0 && !chm.IsEmpty() {
		t.Error("hashmap is not empty")
	}

	// put
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		chm.Put(key, i)
	}

	// replace
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		chm.Replace(key, i+1)
	}

	// test get
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		value := chm.Get(key)
		if i+1 != value {
			t.Errorf("%d != %d", i+1, value)
		}
	}

	for i := 1024; i < 2048; i++ {
		key := strconv.Itoa(i)
		value := chm.Replace(key, i+1)
		if value != nil {
			t.Errorf("replace key %s should fail since key is not present", key)
		}
	}

	if chm.Size() != 1024 {
		t.Errorf("hahsmap size is not equal to %d", 1024)
	}
}

func TestCHashMapRepaceOldWithNew(t *testing.T) {
	chm := NewConcurrentHashMapWithDefaultConfig()

	// put
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		chm.Put(key, i)
	}

	// should succeed to replace
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		replaced := chm.ReplaceOldWithNew(key, i, i+1)

		if !replaced {
			t.Errorf("fail to replace key %s associated with old value %d with new value %d", key, i, i+1)
		}
	}

	// should fail to replace
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		replaced := chm.ReplaceOldWithNew(key, i, i+1)

		if replaced {
			t.Errorf("should fail to replace key %s associated with old value %d with new value %d", key, i, i+1)
		}
	}

}

func TestHashMapClear(t *testing.T) {
	chm := NewConcurrentHashMapWithDefaultConfig()

	if chm.Size() != 0 && !chm.IsEmpty() {
		t.Error("hashmap is not empty")
	}

	// put
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		chm.Put(key, i)
	}

	if chm.Size() != 1024 {
		t.Errorf("hashmap size is not equal to %d", 1024)
	}

	chm.Clear()

	// test contains
	for i := 0; i < 1024; i++ {
		key := strconv.Itoa(i)
		if chm.ContainsKey(key) {
			t.Errorf("still contain key %s after Clear", key)
		}
	}

	if chm.Size() != 0 && !chm.IsEmpty() {
		t.Error("hashmap is not empty after Clear")
	}
}

/*
func BenchmarkCHashMapGet(b *testing.B) {
	b.StopTimer()
	chm := NewConcurrentHashMapWithDefaultConfig()
	chm.Put("foo", "bar")
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		chm.Get("foo")
	}
}

func BenchmarkRWMutexMapGet(b *testing.B) {
	b.StopTimer()
	m := map[string]string{
		"foo": "bar",
	}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.RLock()
		_, _ = m["foo"]
		mu.RUnlock()
	}
}

func BenchmarkCHashMapGetConcurrent(b *testing.B) {
	b.StopTimer()
	chm := NewConcurrentHashMapWithDefaultConfig()
	chm.Put("foo", "bar")
	wg := new(sync.WaitGroup)
	workers := runtime.NumCPU()
	each := b.N / workers
	wg.Add(workers)
	b.StartTimer()
	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < each; j++ {
				chm.Get("foo")
			}
			wg.Done()
		}()
	}
	wg.Wait()
}
func BenchmarkRWMutexMapGetConcurrent(b *testing.B) {
	b.StopTimer()
	m := map[string]string{
		"foo": "bar",
	}
	mu := sync.RWMutex{}
	wg := new(sync.WaitGroup)
	workers := runtime.NumCPU()
	each := b.N / workers
	wg.Add(workers)
	b.StartTimer()
	for i := 0; i < workers; i++ {
		go func() {
			for j := 0; j < each; j++ {
				mu.RLock()
				_, _ = m["foo"]
				mu.RUnlock()
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

func BenchmarkCHashMapGetManyConcurrent(b *testing.B) {
	b.StopTimer()
	n := 10000
	chm := NewConcurrentHashMapWithDefaultConfig()
	keys := make([]string, n)
	for i := 0; i < n; i++ {
		k := "foo" + strconv.Itoa(n)
		keys[i] = k
		chm.Put(k, "bar")
	}
	each := b.N / n
	wg := new(sync.WaitGroup)
	wg.Add(n)
	for _, v := range keys {
		go func() {
			for j := 0; j < each; j++ {
				chm.Get(v)
			}
			wg.Done()
		}()
	}
	b.StartTimer()
	wg.Wait()
}

func BenchmarkCHashMapSet(b *testing.B) {
	b.StopTimer()
	chm := NewConcurrentHashMapWithDefaultConfig()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		chm.Put("foo", "bar")
	}
}
func BenchmarkRWMutexMapSet(b *testing.B) {
	b.StopTimer()
	m := map[string]string{}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m["foo"] = "bar"
		mu.Unlock()
	}
}

func BenchmarkCHashMapSetDelete(b *testing.B) {
	b.StopTimer()
	chm := NewConcurrentHashMapWithDefaultConfig()
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		chm.Put("foo", "bar")
		chm.Remove("foo")
	}
}
func BenchmarkRWMutexMapSetDelete(b *testing.B) {
	b.StopTimer()
	m := map[string]string{}
	mu := sync.RWMutex{}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m["foo"] = "bar"
		mu.Unlock()
		mu.Lock()
		delete(m, "foo")
		mu.Unlock()
	}
}

*/
