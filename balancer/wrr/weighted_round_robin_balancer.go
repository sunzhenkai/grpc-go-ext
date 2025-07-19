package wrr

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

const (
	Name          = "x_weight_round_robin"
	cacheTTL      = 30 * time.Second
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
			weight:  defaultWeight,
		})
	}
	picker := &weightPicker{
		subConns: scs,
		client:   b.client,
	}
	picker.updateWeights()
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

func (p *weightPicker) updateWeights() {
	now := time.Now().Unix()
	if now-atomic.LoadInt64(&p.lastUpdate) < int64(cacheTTL.Seconds()) {
		return
	}

	var total int32
	var wg sync.WaitGroup
	wg.Add(len(p.subConns))

	for _, sc := range p.subConns {
		go func(s *weightSC) {
			defer wg.Done()
			weight := fetchWeight(p.client, s.address)
			atomic.StoreInt32(&s.weight, int32(weight))
		}(sc)
	}
	wg.Wait()

	p.mu.Lock()
	defer p.mu.Unlock()
	for _, sc := range p.subConns {
		total += atomic.LoadInt32(&sc.weight)
	}
	atomic.StoreInt32(&p.total, total)
	atomic.StoreInt64(&p.lastUpdate, now)
}

func fetchWeight(client *http.Client, addr string) int {
	for range maxRetry {
		resp, err := client.Get("http://" + addr + "/rpc/weightfactor")
		if err == nil {
			defer resp.Body.Close()
			var result struct {
				Weight int `json:"weight"`
			}
			if json.NewDecoder(resp.Body).Decode(&result) == nil {
				return max(1, result.Weight)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return defaultWeight
}

func init() {
	balancer.Register(base.NewBalancerBuilder(
		Name,
		&weightPickerBuilder{client: &http.Client{Timeout: 1 * time.Second}},
		base.Config{HealthCheck: true},
	))
}
