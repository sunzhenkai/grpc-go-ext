package weighted

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"sync"
	"time"
)

var weightBalanerRefreshInterval = 60 * time.Second

type WeightedPicker struct {
	nodes      []string
	weights    []int32
	cumulative []int32
	total      int32
	manager    WeightManagerIf
	rand       *rand.Rand
	mu         sync.RWMutex
	ctx        context.Context

	ticker *time.Ticker
	stopCh chan struct{}
	stopWg sync.WaitGroup
}

func NewWeightedPicker(nodes []string, manager WeightManagerIf, ctx context.Context) *WeightedPicker {
	wb := &WeightedPicker{
		nodes:   nodes,
		manager: manager,
		rand:    rand.New(rand.NewSource(time.Now().UnixNano())),
		ctx:     ctx,
	}
	wb.updateWeights()
	return wb
}

func (wb *WeightedPicker) updateWeights() {
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

func (wb *WeightedPicker) StartAutoUpdate() {
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
			case <-wb.ctx.Done():
				return
			}
		}
	}()
	log.Printf("WeightedPicker: quit auto update. [nodes=%v]", wb.nodes)
}

func (wb *WeightedPicker) Stop() {
	if wb.ticker == nil {
		return
	}
	wb.ticker.Stop()
	close(wb.stopCh)
	wb.stopWg.Wait()
	wb.ticker = nil
	wb.stopCh = nil
}

func (wb *WeightedPicker) Pick() (string, error) {
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

func (wb *WeightedPicker) UpdateNodes(nodes []string) {
	wb.mu.Lock()
	wb.nodes = nodes
	wb.mu.Unlock()
	wb.updateWeights()
}
