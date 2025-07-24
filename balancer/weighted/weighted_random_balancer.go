package weighted

import (
	"fmt"

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

type pickerBuilder struct{}

func (b *pickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	nodes := make([]string, 0, len(info.ReadySCs))
	scToAddr := make(map[string]balancer.SubConn)

	for sc, sci := range info.ReadySCs {
		addrStr := sci.Address.Addr
		nodes = append(nodes, addrStr)
		scToAddr[addrStr] = sc
	}

	manager := GetWeightManager()
	p := NewWeightedPicker(nodes, manager)
	// FIXME: this will cause coroutine leak
	// wb.StartAutoUpdate()

	return &weightedPicker{
		picker:     p,
		subConnMap: scToAddr,
	}
}

type weightedPicker struct {
	picker     *WeightedPicker
	subConnMap map[string]balancer.SubConn
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
