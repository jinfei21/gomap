package cmap

import (
	//"runtime"
	"strconv"
	"sync"
	"testing"
)

func TestSegmentPutAndGet(t *testing.T) {
	m := newSegment(1024, 0.75)

	// put
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		m.put(key, getHash(key), i, false)
	}

	// test get
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		value := m.get(key, getHash(key))
		if i != value {
			t.Errorf("%d == %d", i, value)
		}
	}

	// test contains
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		if !m.containsKey(key, getHash(key)) {
			t.Errorf("not contain key %s", key)
		}
	}

	// put with value replacement
	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i)
		m.put(key, getHash(key), i+1, false)
	}

	// test get
	for i := 0; i < 1000; i++ {
		key := strconv.Itoa(i)
		value := m.get(key, getHash(key))
		if i+1 != value {
			t.Errorf("%d != %d", i+1, value)
		}
	}

	if m.size() != 1000 {
		t.Error("the size of segment is not 1000")
	}
}

func TestSegmentPutIfAbsent(t *testing.T) {
	m := newSegment(1024, 0.75)

	// put
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		if i%2 == 0 {
			old := m.put(key, getHash(key), i, true)
			if old != nil {
				t.Errorf("fail to put key %s which is aready present in the segment", key)
			}
		}
	}

	if m.size() != 50 {
		t.Error("the size of segment is not 50")
	}

	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		if i%2 == 0 {
			old := m.put(key, getHash(key), i, true)
			if old != i {
				t.Errorf("existing value mismatch, %d != %d", old, i)
			}
		} else {
			old := m.put(key, getHash(key), i, true)
			if old != nil {
				t.Errorf("fail to put key %s which is aready present in the segment", key)
			}
		}
	}

	if m.size() != 100 {
		t.Error("the size of segment is not 100")
	}

}

func TestSegmentSizeAndRemove(t *testing.T) {
	m := newSegment(1024, 0.75)

	if m.size() != 0 && !m.isEmpty() {
		t.Error("segment is not empty")
	}

	// put
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		m.put(key, getHash(key), i, false)
	}

	if m.isEmpty() {
		t.Error("segment is empty")
	}
	if m.size() != 100 {
		t.Errorf("segment size is not equal to %d", 100)
	}

	// remove
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		value := m.remove(key, getHash(key), nil)
		if i != value {
			t.Errorf("%d == %d", i, value)
		}
	}

	// test contains
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		if m.containsKey(key, getHash(key)) {
			t.Errorf("still contain key %s after removal", key)
		}
	}

	if m.size() != 0 && !m.isEmpty() {
		t.Error("segment is not empty after removal")
	}
}

func TestSegmentRemoveIfValuePresent(t *testing.T) {
	m := newSegment(1024, 0.75)

	// put
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		if i%2 == 0 {
			old := m.put(key, getHash(key), i, true)
			if old != nil {
				t.Errorf("fail to put key %s which is aready present in the segment", key)
			}
		} else {
			old := m.put(key, getHash(key), i+1, true)
			if old != nil {
				t.Errorf("fail to put key %s which is aready present in the segment", key)
			}
		}
	}

	// remove
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		if i%2 == 0 {
			old := m.remove(key, getHash(key), i) // remove value present
			if old != i {
				t.Errorf("removed value mismatch, %d != %d", old, i)
			}
		} else {
			old := m.remove(key, getHash(key), i) // remove value absent
			if old != nil {
				t.Errorf("remove key %s should fail because of value mismatch", key)
			}
		}
	}
}

func TestSegmentRepace(t *testing.T) {
	m := newSegment(1024, 0.75)

	if m.size() != 0 && !m.isEmpty() {
		t.Error("segment is not empty")
	}

	// put
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		m.put(key, getHash(key), i, false)
	}

	// replace
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		m.replace(key, getHash(key), i+1)
	}

	// test get
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		value := m.get(key, getHash(key))
		if i+1 != value {
			t.Errorf("%d != %d", i+1, value)
		}
	}

	for i := 100; i < 200; i++ {
		key := strconv.Itoa(i)
		value := m.replace(key, getHash(key), i+1)
		if value != nil {
			t.Errorf("replace key %s should fail since key is not present", key)
		}
	}

	if m.size() != 100 {
		t.Errorf("segment size is not equal to %d", 100)
	}
}

func TestSegmentRepaceOldWithNew(t *testing.T) {
	m := newSegment(1024, 0.85)

	// put
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		m.put(key, getHash(key), i, false)
	}

	// should succeed to replace
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		replaced := m.replaceOldWithNew(key, getHash(key), i, i+1)

		if !replaced {
			t.Errorf("fail to replace key %s associated with old value %d with new value %d", key, i, i+1)
		}
	}

	// should fail to replace
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		replaced := m.replaceOldWithNew(key, getHash(key), i, i+1)

		if replaced {
			t.Errorf("should fail to replace key %s associated with old value %d with new value %d", key, i, i+1)
		}
	}

}

func TestSegmentClear(t *testing.T) {
	m := newSegment(1024, 0.75)

	if m.size() != 0 && !m.isEmpty() {
		t.Error("segment is not empty")
	}

	// put
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		m.put(key, getHash(key), i, false)
	}

	if m.size() != 100 {
		t.Errorf("segment size is not equal to %d", 100)
	}

	m.clear()

	// test contains
	for i := 0; i < 100; i++ {
		key := strconv.Itoa(i)
		if m.containsKey(key, getHash(key)) {
			t.Errorf("still contain key %s after clear", key)
		}
	}

	if m.size() != 0 && !m.isEmpty() {
		t.Error("segment is not empty after clear")
	}
}

func TestSegmentRehash(t *testing.T) {
	m := newSegment(1024, 0.75)

	// put
	for i := 0; i < 1024*128; i++ {
		key := strconv.Itoa(i)
		m.put(key, getHash(key), i, false)
	}

	if m.rehashCount != 8 {
		t.Errorf("rehashing count %d is not equal to %d", m.rehashCount, 8)
	}

	// get
	for i := 0; i < 1024*128; i++ {
		key := strconv.Itoa(i)
		value := m.remove(key, getHash(key), nil)
		if i != value {
			t.Errorf("%d == %d", i, value)
		}
	}

}

func BenchmarkSegmentMapGet(b *testing.B) {
	b.StopTimer()
	s := newSegment(1024, 0.75)
	s.put("foo", 123, "bar", false)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		s.get("foo", 123)
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
