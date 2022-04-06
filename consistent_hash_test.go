package consistent_hash

import (
	"testing"
)

type testNode struct {
	key string
}

func (t testNode) Key() string {
	return t.key
}

func TestConsistentHash_Add(t *testing.T) {

}
