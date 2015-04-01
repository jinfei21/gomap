// A Simplified Golang Implmentation of JDK ConcurrentHashMap,
// The basic strategy is to subdivide the table among Segments,
// each of which itself is a concurrently readable hash table.
package cmap

import (
	"fmt"
	"hash/fnv"
	"math"
	"sync"
)

const (
	// The default initial capacity for this table,
	// used when not otherwise specified in a constructor.
	DEFAULT_INITIAL_CAPACITY int = 16

	// The default load factor for this table, used when not
	// otherwise specified in a constructor.
	DEFAULT_LOAD_FACTOR float32 = 0.75

	// The default concurrency level for this table, used when not
	// otherwise specified in a constructor.
	DEFAULT_CONCURRENCY_LEVEL = 16
	// The maximum capacity, used if a higher value is implicitly
	// sepcified by either of the constructor with arguments. MUST
	// be a power of two <= 1<<30 to ensure that entries are indexable
	// using ints.
	MAXIMUM_CAPACITY int = 1 << 30
	// The maximum number of segments to allow; used to bound
	// constructor arguments.
	MAXIMUM_SEGMENTS int = 1 << 16 // slightly conservative
)

// concurrent hashmap interface
type concurrentHashMapInterface interface {
	// Maps the specified key to the specified value in the table.
	// Neither the key nor the value can be nil.
	Put(string, interface{}) interface{}
	// Maps the specified key to the specified value in the table,
	// only if the corresponding key/value entry is absent in the table.
	PutIfAbsent(string, interface{}) interface{}
	// Returns the value to which the specified key is mapped,
	// or nil if this map contains no mapping for the key,
	Get(string) interface{}
	// Removes the key (and its corresponding value) from this map.
	// This method does nothing if the key is not in the map.
	Remove(string) interface{}
	// Removes the key (and its corresponding value) from this map,
	// only if the corresponding key/value entry is present in the table.
	RemoveIfPresent(string, interface{}) bool
	// Replace old value of an existing mapping with a new one,
	// only if the old value exists in the mapping.
	ReplaceOldWithNew(string, interface{}, interface{}) bool
	// Repalce old value of an existing mapping with a new one,
	// regardless of the old value
	Replace(string, interface{}) interface{}
	// Checks if the map contains specified key
	ContainsKey(string) bool
	// Returns the number of key/value mappings in this map.
	Size() int
	// Removes all of the mappings from this map.
	Clear()
	// Returns true if this map contains no key/value mappings.
	IsEmpty() bool
}

// Hash map entry
type hashEntry struct {
	key   string
	hash  int
	value interface{}
	next  *hashEntry
}

// Segments are specialied versions of hash tables.
type segment struct {
	sync.RWMutex
	// The number of elements in this segment's region.
	count int
	// The number of rehashing happened on this segment
	// for testing only.
	rehashCount int
	// The table is rehashed when its size exceeds this threshold.
	// The value of this field is always (capacity * loadFactor).
	threshold int
	// The per-segment table.
	table []*hashEntry
	// The load factor for the hash table.
	loadFactor float32
}

func newSegment(initialCapacity int, lf float32) *segment {
	s := &segment{
		loadFactor: lf,
		threshold:  int(float32(initialCapacity) * lf),
		table:      make([]*hashEntry, initialCapacity),
	}
	return s
}

// Returns properly casted first entry of bin for given hash.
func (s *segment) getFirst(hash int) *hashEntry {
	tab := s.table
	return tab[hash&(len(tab)-1)]
}

func (s *segment) get(key string, hash int) interface{} {
	if s.count == 0 {
		return nil
	}
	s.RLock()

	e := s.getFirst(hash)
	for e != nil {
		if e.hash == hash && key == e.key {
			s.RUnlock()
			return e.value
		}
		e = e.next
	}

	s.RUnlock()

	return nil
}

func (s *segment) containsKey(key string, hash int) bool {
	if s.count == 0 {
		return false
	}
	s.RLock()
	e := s.getFirst(hash)
	for e != nil {
		if e.hash == hash && key == e.key {
			s.RUnlock()
			return true
		}
		e = e.next
	}

	s.RUnlock()

	return false
}

func (s *segment) replaceOldWithNew(key string, hash int, oldValue interface{}, newValue interface{}) bool {
	s.Lock()
	e := s.getFirst(hash)
	for e != nil {
		if e.hash == hash && key == e.key {
			break
		}
		e = e.next
	}

	replaced := false
	if e != nil && oldValue == e.value {
		e.value = newValue
		replaced = true
	}

	s.Unlock()
	return replaced
}

func (s *segment) replace(key string, hash int, newValue interface{}) interface{} {
	s.Lock()
	e := s.getFirst(hash)
	for e != nil {
		if e.hash == hash && key == e.key {
			break
		}
		e = e.next
	}

	var oldValue interface{} = nil
	if e != nil {
		oldValue = e.value
		e.value = newValue
	}

	s.Unlock()
	return oldValue
}

func (s *segment) put(key string, hash int, value interface{}, onlyIfAbsent bool) interface{} {
	s.Lock()
	c := s.count
	if c > s.threshold {
		s.rehash()
	}
	c += 1
	tab := s.table
	index := hash & (len(tab) - 1)
	first := tab[index]
	e := first
	for e != nil {
		if e.hash == hash && e.key == key {
			break
		}
		e = e.next
	}

	var oldValue interface{} = nil
	if e != nil {
		oldValue = e.value
		if !onlyIfAbsent {
			e.value = value
		}
	} else {
		tab[index] = &hashEntry{key, hash, value, first}
		s.count = c
	}
	s.Unlock()
	return oldValue

}

func (s *segment) remove(key string, hash int, value interface{}) interface{} {
	s.Lock()
	c := s.count - 1
	tab := s.table
	index := hash & (len(tab) - 1)
	e := tab[index]
	for e != nil {
		if e.hash == hash && e.key == key {
			break
		}
		e = e.next
	}
	var oldValue interface{} = nil
	if e != nil {
		if value == nil || value == e.value {
			oldValue = e.value
			removeEntry(tab, index, e)
			s.count = c
		}
	}

	s.Unlock()
	return oldValue
}

func removeEntry(tab []*hashEntry, index int, e *hashEntry) {
	first := tab[index]

	if first == e {
		tab[index] = e.next
		e.next = nil // ready for GC
	} else {
		p := first
		for p.next != e {
			p = p.next
		}
		p.next = e.next
		e.next = nil // ready for GC
	}
}

func (s *segment) rehash() {
	oldTable := s.table
	oldCapacity := len(oldTable)
	if oldCapacity >= MAXIMUM_CAPACITY {
		return
	}

	newTable := make([]*hashEntry, oldCapacity<<1)
	s.threshold = int(float32(len(newTable)) * s.loadFactor)
	sizeMask := len(newTable) - 1
	for _, e := range oldTable {
		if e != nil {
			p := e
			q := e.next

			for {
				k := p.hash & sizeMask
				p.next = newTable[k]
				newTable[k] = p

				p = q
				if p == nil {
					break
				} else {
					q = p.next
				}
			}
		}
	}
	s.rehashCount += 1
	s.table = newTable
}

func (s *segment) clear() {
	if s.count != 0 {
		s.Lock()
		tab := s.table
		for i := 0; i < len(tab); i++ {
			tab[i] = nil
		}
		s.count = 0
		s.Unlock()
	}
}

// for test
func (s *segment) size() int {
	return s.count
}

// for test
func (s *segment) isEmpty() bool {
	return s.count == 0
}

type MapConfig struct {
	InitialCapacity  int
	ConcurrencyLevel int
	LoadFactor       float32
}

type ConcurrentHashMap struct {
	// Mask value for indexing into segments. The upper bits of a
	// key's hash code are used to choose a segment.
	segmentMask uint
	// shift value for indexing within segments.
	segmentShift uint
	// The segments, each of which is a specified hash table.
	segments []*segment
}

func NewConcurrentHashMapWithDefaultConfig() *ConcurrentHashMap {
	CHM, _ := NewConcurrentHashMapWithConfig(MapConfig{})
	return CHM
}

// Create a new, empty map with the specified initial
// capacity, load factor, and concurrency level.
func NewConcurrentHashMapWithConfig(config MapConfig) (*ConcurrentHashMap, error) {
	if config.LoadFactor < 0 {
		return nil, fmt.Errorf("load factor %f is invalid", config.LoadFactor)
	}
	if config.InitialCapacity < 0 {
		return nil, fmt.Errorf("initial capacity %d is invalid", config.InitialCapacity)
	}
	if config.ConcurrencyLevel < 0 {
		return nil, fmt.Errorf("concurrency level %d is invalid", config.ConcurrencyLevel)
	}
	if math.Abs(float64(config.LoadFactor)-0.0) < 1e-6 {
		config.LoadFactor = DEFAULT_LOAD_FACTOR
	}
	if config.InitialCapacity == 0 {
		config.InitialCapacity = DEFAULT_INITIAL_CAPACITY
	}
	if config.ConcurrencyLevel == 0 {
		config.ConcurrencyLevel = DEFAULT_CONCURRENCY_LEVEL
	}
	if config.ConcurrencyLevel > MAXIMUM_SEGMENTS {
		config.ConcurrencyLevel = MAXIMUM_SEGMENTS
	}
	if config.InitialCapacity > MAXIMUM_CAPACITY {
		config.InitialCapacity = MAXIMUM_CAPACITY
	}
	// Find power-of-two sizes best matching arguments
	sshift := 0
	ssize := 1
	for ssize < config.ConcurrencyLevel {
		sshift += 1
		ssize <<= 1
	}

	CHM := &ConcurrentHashMap{
		segmentShift: uint(32 - sshift),
		segmentMask:  uint(ssize - 1),
		segments:     make([]*segment, ssize),
	}

	c := config.InitialCapacity / ssize
	if c*ssize < config.InitialCapacity {
		c += 1
	}
	capacity := 1
	for capacity < c {
		capacity <<= 1
	}

	for i := 0; i < len(CHM.segments); i++ {
		CHM.segments[i] = newSegment(capacity, config.LoadFactor)
	}

	return CHM, nil
}

// Return the segment that should be used for key with given hash
func (m *ConcurrentHashMap) segmentFor(hash int) *segment {
	return m.segments[(uint(hash)>>m.segmentShift)&m.segmentMask]
}

// Returns true if this map contains no key-value mappings.
func (m *ConcurrentHashMap) IsEmpty() bool {
	for _, s := range m.segments {
		if s.count > 0 {
			return false
		}
	}
	return true
}

func (m *ConcurrentHashMap) Clear() {
	for _, s := range m.segments {
		s.clear()
	}
}

// Return the number of key-value mappings in this map. If the
// map contains more than math.MaxInt32 elements, returns
// math.MaxInt32
func (m *ConcurrentHashMap) Size() int {
	var total int64 = 0
	for _, s := range m.segments {
		total += int64(s.count)
	}

	if int64(math.MaxInt32) < total {
		return math.MaxInt32
	} else {
		return int(total)
	}
}

func getHash(key string) int {
	h := fnv.New32()
	h.Write([]byte(key))
	return int(h.Sum32())
}

// Returns the value to which the specified key is mapped,
// or nil if this map contains no mapping for the key.
func (m *ConcurrentHashMap) Get(key string) interface{} {
	hash := getHash(key)
	return m.segmentFor(hash).get(key, hash)
}

// Tests if the specified key is present in this table.
func (m *ConcurrentHashMap) ContainsKey(key string) bool {
	hash := getHash(key)
	return m.segmentFor(hash).containsKey(key, hash)
}

// Maps the specified key to the specified value in this table.
// If the map previously contained a mapping for the key, the old value is
// replaced by the specified value.
// The value can be retrieved by calling the Get method
// with a key that is equal to the original key.
func (m *ConcurrentHashMap) Put(key string, value interface{}) interface{} {
	hash := getHash(key)
	return m.segmentFor(hash).put(key, hash, value, false)
}

// Maps the specified key to the specified value in this table only if the key is absent.
// The value can be retrieved by calling the Get method
// with a key that is equal to the original key.
func (m *ConcurrentHashMap) PutIfAbsent(key string, value interface{}) interface{} {
	hash := getHash(key)
	return m.segmentFor(hash).put(key, hash, value, true)
}

// Removes the key (and its corresponding value) from this map.
// This method does nothing if the key is not in the map.
func (m *ConcurrentHashMap) Remove(key string) interface{} {
	hash := getHash(key)
	return m.segmentFor(hash).remove(key, hash, nil)
}

// Remove entry for key only if currently mapped to a given value.
// This method does nothing(return false) if the key is not in the map.
func (m *ConcurrentHashMap) RemoveIfPresent(key string, value interface{}) bool {
	hash := getHash(key)
	return m.segmentFor(hash).remove(key, hash, value) != nil
}

// Replace entry for key only if currently mapped to a given value.
// This method does nothing(return false) if the key or the old value is not in the map.
func (m *ConcurrentHashMap) ReplaceOldWithNew(key string, oldValue interface{}, newValue interface{}) bool {
	hash := getHash(key)
	return m.segmentFor(hash).replaceOldWithNew(key, hash, oldValue, newValue)
}

// Replace entry for key only if currently mapped to some value.
// This method does nothing(return nil) if the key is not in the map.
func (m *ConcurrentHashMap) Replace(key string, value interface{}) interface{} {
	hash := getHash(key)
	return m.segmentFor(hash).replace(key, hash, value)
}
