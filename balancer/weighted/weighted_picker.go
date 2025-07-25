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
	log.Printf("WeightedPicker: Weights updated. Nodes: %v, Total Weight: %d", wb.nodes, wb.total)
}

func (wb *WeightedPicker) StartAutoUpdate() {
	select {
	case <-wb.ctx.Done():
		log.Printf("WeightedPicker: Context already cancelled, not starting auto update. [nodes=%v]", wb.nodes)
		return
	default:
	}

	ticker := time.NewTicker(weightBalanerRefreshInterval)
	defer ticker.Stop()

	go func() {
		for {
			select {
			case <-ticker.C:
				wb.updateWeights()
			case <-wb.ctx.Done():
				log.Printf("WeightedPicker: quit auto update (context quit). [nodes=%v]", wb.nodes)
				return
			}
		}
	}()
}

func (wb *WeightedPicker) Stop() {
	log.Printf("WeightedPicker: Stop called. Auto update will stop when context is cancelled by caller.")
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
	log.Printf("WeightedPicker: Nodes updated. Triggering async weight update. New nodes: %v", nodes)
}
