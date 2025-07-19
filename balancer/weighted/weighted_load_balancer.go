package weighted

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

type WeightedBalancer struct {
	nodes      []string
	weights    []int32
	cumulative []int32 // 新增：用于二分查找
	total      int32
	manager    WeightFactorManagerIf
	rand       *rand.Rand
	mu         sync.RWMutex // 用 RWMutex 提升 Pick 并发性能
}

func NewWeightedBalancer(nodes []string, manager WeightFactorManagerIf) *WeightedBalancer {
	wb := &WeightedBalancer{
		nodes:   nodes,
		manager: manager,
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	wb.updateWeights()
	return wb
}

func (wb *WeightedBalancer) updateWeights() {
	n := len(wb.nodes)

	newWeights := make([]int32, n)
	newCumulative := make([]int32, n)
	var newTotal int32

	for i, node := range wb.nodes {
		weight := wb.manager.GetAddressWeight(node)
		newWeights[i] = weight
		newTotal += weight
		newCumulative[i] = newTotal
	}

	wb.mu.Lock()
	defer wb.mu.Unlock()

	wb.weights = newWeights
	wb.cumulative = newCumulative
	wb.total = newTotal
}

func (wb *WeightedBalancer) Pick() (string, error) {
	wb.mu.RLock()
	defer wb.mu.RUnlock()

	if len(wb.nodes) == 0 || wb.total == 0 {
		return "", fmt.Errorf("no available nodes in the balancer")
	}

	r := wb.rand.Int31n(wb.total)
	idx := sort.Search(len(wb.cumulative), func(i int) bool {
		return r < wb.cumulative[i]
	})
	if idx < len(wb.nodes) {
		return wb.nodes[idx], nil
	}
	randomIdx := int(r) % len(wb.nodes)
	return wb.nodes[randomIdx], nil
}

func (wb *WeightedBalancer) UpdateNodes(nodes []string) {
	wb.mu.Lock()
	defer wb.mu.Unlock()
	wb.nodes = nodes
	wb.updateWeights()
}
