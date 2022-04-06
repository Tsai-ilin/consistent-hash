package consistent_hash

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

type Node interface {
	Key() string
}

type consistentNode struct {
	node         Node
	virtualNodes []uint32
}

type ConsistentHash struct {
	hashSortedNodes []uint32
	circle          map[uint32]string
	nodes           map[string]consistentNode
	sync.RWMutex
	hash func(string) uint32
}

func NewConsistentHash() *ConsistentHash {
	return &ConsistentHash{}
}

func NewConsistentWithCustomHash(h func(key string) uint32) *ConsistentHash {
	return &ConsistentHash{hash: h}
}

func (c *ConsistentHash) hashKey(key string) uint32 {
	return c.hash(key)
}

func (c *ConsistentHash) Add(node Node) error {
	return c.AddWithVirtualNode(node, 1)
}

func (c *ConsistentHash) AddWithVirtualNode(node Node, virtualNodeCount int) error {
	if node == nil {
		return errors.New("node is nil")
	}

	if virtualNodeCount < 1 {
		return errors.New("virtualNodeCount can't less 1")
	}
	c.Lock()
	defer c.Unlock()

	if c.circle == nil {
		c.circle = map[uint32]string{}
	}
	if c.nodes == nil {
		c.nodes = map[string]consistentNode{}
	}
	if c.hash == nil {
		c.hash = func(key string) uint32 {
			return crc32.ChecksumIEEE([]byte(key))
		}
	}

	if _, ok := c.nodes[node.Key()]; ok {
		return fmt.Errorf("node %s already exised", node.Key())
	}

	// 添加虚拟结点
	var virtualNodes []uint32
	for i := 0; i < virtualNodeCount; i++ {
		var virtualKey *uint32
		for j := 0; j < 3; j++ { // 防止 hash 冲突，重试 3 次
			k := c.hashKey(node.Key() + strconv.Itoa(i) + strconv.Itoa(j))
			_, ok := c.circle[k]
			if !ok {
				virtualKey = &k
				break
			}
		}
		if virtualKey == nil {
			// 重试三次还是冲突
			return fmt.Errorf("node %s hash collision", node.Key())
		}
		c.circle[*virtualKey] = node.Key()
		virtualNodes = append(virtualNodes, *virtualKey)
	}
	c.hashSortedNodes = append(c.hashSortedNodes, virtualNodes...)
	c.nodes[node.Key()] = consistentNode{node: node, virtualNodes: virtualNodes}

	//虚拟结点排序
	sort.Slice(c.hashSortedNodes, func(i, j int) bool {
		return c.hashSortedNodes[i] < c.hashSortedNodes[j]
	})

	return nil
}

func (c *ConsistentHash) Remove(node Node) error {
	c.Lock()
	defer c.Unlock()
	cNode, ok := c.nodes[node.Key()]
	if !ok {
		return fmt.Errorf("node %s not exist", node.Key())
	}
	delete(c.nodes, node.Key())

	// Add 方法保证了此处不需要考虑 hash 冲突
	for _, v := range cNode.virtualNodes {
		delete(c.circle, v)
	}

	// 二分查找删除
	for _, v := range cNode.virtualNodes {
		i := sort.Search(len(c.hashSortedNodes), func(i int) bool {
			return c.hashSortedNodes[i] >= v
		})
		c.hashSortedNodes = append(c.hashSortedNodes[:i], c.hashSortedNodes[i+1:]...)
	}
	return nil
}

func (c *ConsistentHash) GetNode(key string) (Node, error) {
	c.RLock()
	defer c.RUnlock()

	if len(c.nodes) == 0 {
		return nil, errors.New("node size is 0")
	}
	hash := c.hashKey(key)
	i := c.getPosition(hash)

	return c.nodes[c.circle[c.hashSortedNodes[i]]].node, nil
}

func (c *ConsistentHash) getPosition(hash uint32) int {
	i := sort.Search(len(c.hashSortedNodes), func(i int) bool { return c.hashSortedNodes[i] >= hash })

	if i < len(c.hashSortedNodes) {
		if i == len(c.hashSortedNodes)-1 {
			return 0
		} else {
			return i
		}
	} else {
		return len(c.hashSortedNodes) - 1
	}
}
