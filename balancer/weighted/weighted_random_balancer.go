package weighted

import (
	"context"
	"fmt"
	"log"
	"sort"
	"sync"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

const (
	Name = "x_weighted_random"
)

func init() {
	balancer.Register(base.NewBalancerBuilder(
		Name,
		&pickerBuilder{},
		base.Config{HealthCheck: true},
	))
}

type pickerBuilder struct {
	mu         sync.Mutex
	prevCancel context.CancelFunc
}

func (b *pickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.prevCancel != nil {
		b.prevCancel()
		b.prevCancel = nil
	}

	nodes := make([]string, 0, len(info.ReadySCs))
	scToAddr := make(map[string]balancer.SubConn)

	for sc, sci := range info.ReadySCs {
		addrStr := sci.Address.Addr
		nodes = append(nodes, addrStr)
		scToAddr[addrStr] = sc
	}
	sort.Strings(nodes)

	manager := GetWeightManager()
	ctx, cancel := context.WithCancel(context.Background())
	p := NewWeightedPicker(nodes, manager, ctx)
	p.StartAutoUpdate()
	b.prevCancel = cancel

	log.Printf("create weighted picker. [ready_scs=%v, nodes=%v]", len(info.ReadySCs), nodes)
	return &weightedPicker{
		picker:     p,
		subConnMap: scToAddr,
		cancel:     cancel,
	}
}

type weightedPicker struct {
	picker     *WeightedPicker
	subConnMap map[string]balancer.SubConn
	cancel     context.CancelFunc
}

func (p *weightedPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	addr, err := p.picker.Pick()
	if err != nil {
		return balancer.PickResult{}, err
	}
	sc, ok := p.subConnMap[addr]
	if !ok {
		return balancer.PickResult{}, fmt.Errorf("subconn not found for addr: %s", addr)
	}
	return balancer.PickResult{SubConn: sc}, nil
}

func (p *weightedPicker) Close() {
	if p.cancel != nil {
		fmt.Printf("weightedPicker: Closing and stopping auto-update for its WeightedPicker.\n")
		p.cancel()
		p.cancel = nil
	}
}
