package wrr

import (
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sunzhenkai/grpc-go-ext/balancer/weighted"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

const (
	Name          = "x_weight_round_robin"
	defaultWeight = 1000
)

type weightPickerBuilder struct {
	client *http.Client
}

func (b *weightPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	scs := make([]*weightSC, 0, len(info.ReadySCs))
	for sc, sci := range info.ReadySCs {
		scs = append(scs, &weightSC{
			sc:      sc,
			address: sci.Address.Addr,
			weight:  weighted.GetWeightManager().GetAddressWeight(sci.Address.Addr),
		})
	}
	picker := &weightPicker{
		subConns: scs,
		client:   b.client,
	}
	return picker
}

type weightSC struct {
	sc      balancer.SubConn
	address string
	weight  int32
}

type weightPicker struct {
	subConns   []*weightSC
	total      int32
	client     *http.Client
	lastUpdate int64
	mu         sync.RWMutex
}

func (p *weightPicker) Pick(info balancer.PickInfo) (balancer.PickResult, error) {
	if atomic.LoadInt32(&p.total) <= 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	target := rand.Int31n(atomic.LoadInt32(&p.total))
	var sum int32
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, sc := range p.subConns {
		sum += atomic.LoadInt32(&sc.weight)
		if target < sum {
			return balancer.PickResult{SubConn: sc.sc}, nil
		}
	}
	return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
}

func init() {
	balancer.Register(base.NewBalancerBuilder(
		Name,
		&weightPickerBuilder{client: &http.Client{Timeout: 1 * time.Second}},
		base.Config{HealthCheck: true},
	))
}
