package weighted

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

var weightBalanerRefreshInterval = 60 * time.Second

type WeightedBalancer struct {
	nodes      []string
	weights    []int32
	cumulative []int32
	total      int32
	manager    WeightManagerIf
	rand       *rand.Rand
	mu         sync.RWMutex

	ticker *time.Ticker
	stopCh chan struct{}
	stopWg sync.WaitGroup
}

func NewWeightedBalancer(nodes []string, manager WeightManagerIf) *WeightedBalancer {
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

func (wb *WeightedBalancer) StartAutoUpdate() {
	if wb.ticker != nil {
		return // already started
	}

	wb.ticker = time.NewTicker(weightBalanerRefreshInterval)
	wb.stopCh = make(chan struct{})

	wb.stopWg.Add(1)
	go func() {
		defer wb.stopWg.Done()
		for {
			select {
			case <-wb.ticker.C:
				wb.updateWeights()
			case <-wb.stopCh:
				return
			}
		}
	}()
}

func (wb *WeightedBalancer) Stop() {
	if wb.ticker == nil {
		return
	}
	wb.ticker.Stop()
	close(wb.stopCh)
	wb.stopWg.Wait()
	wb.ticker = nil
	wb.stopCh = nil
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
	wb.nodes = nodes
	wb.mu.Unlock()
	wb.updateWeights()
}
