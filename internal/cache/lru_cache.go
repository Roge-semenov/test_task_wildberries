package cache

import (
	"sync"
	"test_task_wb/internal/model"
)

// Node - это элемент двусвязного списка, используемого в кэше
type Node struct {
	prev  *Node
	next  *Node
	key   string
	value model.Order
}

// структура LRUCache реализует Least Recently Used кэш
type LRUCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*Node
	head     *Node // Самый "старый" элемент
	tail     *Node // Самый "новый" элемент
}

// NewLRUCache создает новый LRU-кэш с заданной емкостью
func NewLRUCache(capacity int) *LRUCache {
	head := &Node{}
	tail := &Node{}
	head.next = tail
	tail.prev = head

	return &LRUCache{
		capacity: capacity,
		items:    make(map[string]*Node, capacity),
		head:     head,
		tail:     tail,
	}
}

// Set добавляет или обновляет заказ в кэше
func (c *LRUCache) Set(uid string, order model.Order) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, exists := c.items[uid]; exists {
		node.value = order
		c.moveToTail(node)
		return
	}

	if len(c.items) >= c.capacity {
		c.evictOldest()
	}

	newNode := &Node{
		key:   uid,
		value: order,
	}
	c.items[uid] = newNode
	c.addToTail(newNode)
}

// Get получает заказ из кэша
func (c *LRUCache) Get(uid string) (model.Order, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if node, found := c.items[uid]; found {
		c.moveToTail(node)
		return node.value, true
	}

	return model.Order{}, false
}

// addToTail добавляет узел в конец списка (делает его самым новым)
func (c *LRUCache) addToTail(node *Node) {
	prev := c.tail.prev
	prev.next = node
	node.prev = prev
	node.next = c.tail
	c.tail.prev = node
}

// removeNode удаляет узел из списка
func (c *LRUCache) removeNode(node *Node) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

// moveToTail перемещает существующий узел в конец списка
func (c *LRUCache) moveToTail(node *Node) {
	c.removeNode(node)
	c.addToTail(node)
}

// evictOldest удаляет самый старый узел
func (c *LRUCache) evictOldest() {
	oldest := c.head.next
	if oldest == c.tail {
		return // Кэш пуст
	}
	c.removeNode(oldest)
	delete(c.items, oldest.key)
}
