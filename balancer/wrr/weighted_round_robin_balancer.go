package wrr

import (
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
)

var (
	Name           = "weighted_round_robin_http"
	weightHTTPPath = "/rpc/weight"
	defaultWeight  = 1000.0
)

func init() {
	balancer.Register(newBuilder())
}

func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Name, &weightedPickerBuilder{}, base.Config{HealthCheck: true})
}

type weightedPickerBuilder struct{}

func (*weightedPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	log.Printf("WeightedBalancer: Building new Picker. Ready SubConns: %d", len(info.ReadySCs))
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	scWeights := make(map[balancer.SubConn]int)
	scToHTTPAddr := make(map[balancer.SubConn]string)

	for sc, scInfo := range info.ReadySCs {
		gRPCHost, gRPCPortStr, err := net.SplitHostPort(scInfo.Address.Addr)
		if err != nil {
			log.Printf("WeightedBalancer: Invalid gRPC address format for %s: %v. Skipping.", scInfo.Address.Addr, err)
			continue
		}
		gRPCPort, err := strconv.Atoi(gRPCPortStr)
		if err != nil {
			log.Printf("WeightedBalancer: Invalid gRPC port format for %s: %v. Skipping.", scInfo.Address.Addr, err)
			continue
		}
		// httpPort: service port which serving weight api
		httpPort := gRPCPort
		httpAddr := net.JoinHostPort(gRPCHost, strconv.Itoa(httpPort))
		scToHTTPAddr[sc] = httpAddr

		weight := defaultWeight
		weightFactor, err := getWeightFactorFromHTTPTarget(httpAddr)
		if err != nil {
			log.Printf("WeightedBalancer: Failed to get weight for %s (HTTP at %s): %v. Using default weight %d.", scInfo.Address.Addr, httpAddr, err, defaultWeight)
			weight *= weightFactor
		} else {
			log.Printf("WeightedBalancer: Got weight %d for %s (HTTP at %s).", weight, scInfo.Address.Addr, httpAddr)
		}
		scWeights[sc] = int(weight)
	}

	return &weightedPicker{
		scWeights: scWeights,
		r:         rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// getWeightFromHTTPTarget using HTTP GET call /rpc/weight api
func getWeightFactorFromHTTPTarget(httpAddr string) (float64, error) {
	client := http.Client{
		Timeout: 1 * time.Second,
	}
	resp, err := client.Get(fmt.Sprintf("http://%s%s", httpAddr, weightHTTPPath))
	if err != nil {
		return 1.0, fmt.Errorf("could not reach HTTP weight endpoint at %s%s: %v", httpAddr, weightHTTPPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 1.0, fmt.Errorf("HTTP status error from %s%s: %s", httpAddr, weightHTTPPath, resp.Status)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return 1.0, fmt.Errorf("failed to read response body from %s%s: %v", httpAddr, weightHTTPPath, err)
	}

	weightFactor, err := strconv.ParseFloat(string(bodyBytes), 64)
	if err != nil {
		return 1.0, fmt.Errorf("failed to parse weight '%s' from %s%s: %v", string(bodyBytes), httpAddr, weightHTTPPath, err)
	}
	if weightFactor <= 0 {
		return 1.0, fmt.Errorf("received non-positive weight factor %f from %s, using default", weightFactor, httpAddr)
	}
	return weightFactor, nil
}

type weightedPicker struct {
	scWeights map[balancer.SubConn]int
	r         *rand.Rand
	mu        sync.Mutex
}

// Pick load balancer
func (p *weightedPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.PickResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	totalWeight := 0
	var availableSCs []balancer.SubConn

	for sc, weight := range p.scWeights {
		availableSCs = append(availableSCs, sc)
		totalWeight += weight
	}

	if len(availableSCs) == 0 || totalWeight == 0 {
		return balancer.PickResult{}, balancer.ErrNoSubConnAvailable
	}

	pick := p.r.Intn(totalWeight)

	for _, sc := range availableSCs {
		weight := p.scWeights[sc]
		pick -= weight
		if pick < 0 {
			return balancer.PickResult{SubConn: sc}, nil
		}
	}

	log.Printf("WeightedBalancer: Fallback to random pick due to unexpected weighted selection result.")
	return balancer.PickResult{SubConn: availableSCs[p.r.Intn(len(availableSCs))]}, nil
}
