package concurrentHashMap

import (
	"encoding/json"
	"sync"
)

// ConcurrentHashMap 底层是很多的sharedMap，每个sharedMap有一把锁，借此实现分段锁机制
// 将key通过hash函数映射到不同的sharedMap

// ShardCount 底层小shareMap数量
var ShardCount = 32

// ConcurrentHashMap 并发安全的大map，由 ShardCount 个小mao数组组成，方便实现分段锁机制
type ConcurrentHashMap []*SharedMap

// SharedMap 并发安全的小map,ShardCount 个这样的小map数组组成一个大map
type SharedMap struct {
	items        map[string]interface{}
	sync.RWMutex // 读写锁，保护items
}

// New 创建一个新的concurrent map.
func New() ConcurrentHashMap {
	m := make(ConcurrentHashMap, ShardCount)
	for i := 0; i < ShardCount; i++ {
		m[i] = &SharedMap{items: make(map[string]interface{})}
	}
	return m
}

// GetShardMap 返回给定key的sharedMap
func (m ConcurrentHashMap) GetShardMap(key string) *SharedMap {
	return m[uint(fnv32(key))%uint(ShardCount)]
}

// MSet 批量给ConcurrentMap赋值
func (m ConcurrentHashMap) MSet(data map[string]interface{}) {
	for key, value := range data {
		shard := m.GetShardMap(key)
		shard.Lock()
		shard.items[key] = value
		shard.Unlock()
	}
}

// Set 添加 key-value
func (m ConcurrentHashMap) Set(key string, value interface{}) {
	// Get map shard.
	shard := m.GetShardMap(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// UpsertCb 更新元素的先验方法
type UpsertCb func(exist bool, valueInMap interface{}, newValue interface{}) interface{}

// Upsert 根据先验方法的结果，决定是更新还是添加元素
func (m ConcurrentHashMap) Upsert(key string, value interface{}, cb UpsertCb) (res interface{}) {
	shard := m.GetShardMap(key)
	shard.Lock()
	v, ok := shard.items[key]
	res = cb(ok, v, value)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// SetIfAbsent 给定的key不存在就set key-value
func (m ConcurrentHashMap) SetIfAbsent(key string, value interface{}) bool {
	// Get map shard.
	shard := m.GetShardMap(key)
	shard.Lock()
	_, ok := shard.items[key]
	if !ok {
		shard.items[key] = value
	}
	shard.Unlock()
	return !ok
}

// Get 返回指定key的value值
func (m ConcurrentHashMap) Get(key string) (interface{}, bool) {
	shard := m.GetShardMap(key)
	shard.RLock()
	val, ok := shard.items[key]
	shard.RUnlock()
	return val, ok
}

// Count 统计元素总数
func (m ConcurrentHashMap) Count() int {
	count := 0
	for i := 0; i < ShardCount; i++ {
		shard := m[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Has 判断元素是否存在
func (m ConcurrentHashMap) Has(key string) bool {
	// Get shard
	shard := m.GetShardMap(key)
	shard.RLock()
	// See if element is within shard.
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Remove 删除一个元素
func (m ConcurrentHashMap) Remove(key string) {
	// Try to get shard.
	shard := m.GetShardMap(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// RemoveCb 删除的先验方法
type RemoveCb func(key string, v interface{}, exists bool) bool

// RemoveCb 先验方法返回true并且元素存在，则删除元素
func (m ConcurrentHashMap) RemoveCb(key string, cb RemoveCb) bool {
	// Try to get shard.
	shard := m.GetShardMap(key)
	shard.Lock()
	v, ok := shard.items[key]
	remove := cb(key, v, ok)
	if remove && ok {
		delete(shard.items, key)
	}
	shard.Unlock()
	return remove
}

// Pop 获取一个元素的值并删除它
func (m ConcurrentHashMap) Pop(key string) (v interface{}, exists bool) {
	// Try to get shard.
	shard := m.GetShardMap(key)
	shard.Lock()
	v, exists = shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return v, exists
}

// IsEmpty 判断是否为空
func (m ConcurrentHashMap) IsEmpty() bool {
	return m.Count() == 0
}

// Tuple key-value 封装
type Tuple struct {
	Key string
	Val interface{}
}

// Iter 无缓冲的迭代器，可以在range循环中使用，不建议使用，无缓冲channle接收不及时会阻塞
func (m ConcurrentHashMap) Iter() <-chan Tuple {
	chans := snapshot(m)
	ch := make(chan Tuple)
	go fanIn(chans, ch)
	return ch
}

// IterBuffered 返回吐过有缓冲的迭代器，可以在range循环中使用
func (m ConcurrentHashMap) IterBuffered() <-chan Tuple {
	chans := snapshot(m)
	total := 0
	for _, c := range chans {
		total += cap(c)
	}
	ch := make(chan Tuple, total)
	go fanIn(chans, ch)
	return ch
}

// Clear 清空
func (m ConcurrentHashMap) Clear() {
	// todo 直接操作底层sharedMap清空不行
	for item := range m.IterBuffered() {
		m.Remove(item.Key)
	}
}

// 返回一个通道数组，一个share map 内的元素存储到同一个通道
func snapshot(m ConcurrentHashMap) (chans []chan Tuple) {
	if len(m) == 0 {
		panic(`cmap.ConcurrentHashMap is not initialized. Should run New() before usage.`)
	}
	chans = make([]chan Tuple, ShardCount)
	wg := sync.WaitGroup{}
	wg.Add(ShardCount)
	for index, shard := range m {
		// 每个share map 启动一个协程去处理
		go func(index int, shard *SharedMap) {
			// Foreach key, value pair.
			shard.RLock()
			chans[index] = make(chan Tuple, len(shard.items))
			wg.Done()
			for key, val := range shard.items {
				chans[index] <- Tuple{key, val}
			}
			shard.RUnlock()
			close(chans[index])
		}(index, shard)
	}
	wg.Wait()
	return chans
}

// fanIn 从通道数组中读取所有元素进入out通道
func fanIn(chans []chan Tuple, out chan Tuple) {
	wg := sync.WaitGroup{}
	wg.Add(len(chans))
	for _, ch := range chans {
		go func(ch chan Tuple) {
			for t := range ch {
				out <- t
			}
			wg.Done()
		}(ch)
	}
	wg.Wait()
	close(out)
}

// Items 以map的形式返回所有key-value
func (m ConcurrentHashMap) Items() map[string]interface{} {
	tmp := make(map[string]interface{})

	// Insert items to temporary map.
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}

	return tmp
}

// IterCb 回调方法
type IterCb func(key string, v interface{})

// IterCb 对每一个key-value都执行回调
func (m ConcurrentHashMap) IterCb(fn IterCb) {
	for idx := range m {
		shard := (m)[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Keys 以字符串数组的形式返回所有key
func (m ConcurrentHashMap) Keys() []string {
	count := m.Count()
	ch := make(chan string, count)
	go func() {
		// Foreach shard.
		wg := sync.WaitGroup{}
		wg.Add(ShardCount)
		for _, shard := range m {
			go func(shard *SharedMap) {
				// Foreach key, value pair.
				shard.RLock()
				for key := range shard.items {
					ch <- key
				}
				shard.RUnlock()
				wg.Done()
			}(shard)
		}
		wg.Wait()
		close(ch)
	}()

	// Generate keys
	keys := make([]string, 0, count)
	for k := range ch {
		keys = append(keys, k)
	}
	return keys
}

// MarshalJSON 将map转化为json
func (m ConcurrentHashMap) MarshalJSON() ([]byte, error) {
	tmp := make(map[string]interface{})
	for item := range m.IterBuffered() {
		tmp[item.Key] = item.Val
	}
	return json.Marshal(tmp)
}

// fnv32 hash函数
func fnv32(key string) uint32 {
	// 著名的fnv哈希函数，由 Glenn Fowler、Landon Curt Noll和 Kiem-Phong Vo 创建
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
