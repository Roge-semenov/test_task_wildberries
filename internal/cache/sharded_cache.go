package cache

import (
	"hash/fnv"
	"log/slog"
	"test_task_wb/internal/model"
)

type ShardedCache struct {
	shards    []*LRUCache
	numShards uint32
}

// NewShardedCache создает новый сегментированный кэш
func NewShardedCache(capacityPerShard int, numShards int) OrderCache {
	if numShards <= 0 {
		slog.Warn("numShards for cache is zero or negative, defaulting to 1", "provided_value", numShards)
		numShards = 1
	}

	sc := &ShardedCache{
		shards:    make([]*LRUCache, numShards),
		numShards: uint32(numShards),
	}

	for i := 0; i < numShards; i++ {
		sc.shards[i] = NewLRUCache(capacityPerShard)
	}

	return sc
}

// getShardIndex вычисляет, в какой сегмент попадет ключ
func (sc *ShardedCache) getShardIndex(uid string) uint32 {
	hash := fnv.New32a()
	hash.Write([]byte(uid))
	return hash.Sum32() % sc.numShards
}

// Get находит нужный shard и возвращает из него значение
func (sc *ShardedCache) Get(uid string) (model.Order, bool) {
	shardIndex := sc.getShardIndex(uid)
	shard := sc.shards[shardIndex]

	return shard.Get(uid)
}

// Set находит нужный shard и записывает в него значение
func (sc *ShardedCache) Set(uid string, order model.Order) {
	shardIndex := sc.getShardIndex(uid)
	shard := sc.shards[shardIndex]

	shard.Set(uid, order)
}
