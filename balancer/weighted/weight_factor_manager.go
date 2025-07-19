package weighted

import (
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var (
	globalWeightFactorManager *WeightFactorManager
	once                      sync.Once
	weightFactorHTTPPath      = "/rpc/weightfactor"
	weightRefreshInterval     = 60 * time.Second
	defaultWeight             = 1000.0
)

type WeightFactorManager struct {
	weights    sync.Map // map[string]int32
	client     *http.Client
	stopChan   chan struct{}
	updateLock sync.Mutex
}

func newWeightFactorManager() *WeightFactorManager {
	return &WeightFactorManager{
		client:   &http.Client{Timeout: 1 * time.Second},
		stopChan: make(chan struct{}),
	}
}

func (m *WeightFactorManager) GetAddressWeight(endpoint string) int32 {
	if weight, ok := m.weights.Load(endpoint); ok {
		return weight.(int32)
	}

	if weightFactor, err := fetchWeightFactor(m.client, endpoint); err == nil {
		weight := int32(weightFactor * defaultWeight)
		m.weights.Store(endpoint, weight)
		return weight
	} else {
		return int32(defaultWeight)
	}
}

func (m *WeightFactorManager) RemoveAddress(addrs ...string) {
	for _, addr := range addrs {
		if _, ok := m.weights.Load(addr); ok {
			m.weights.Delete(addr)
		}
	}
}

func (m *WeightFactorManager) start() {
	go m.refreshLoop()
}

func (m *WeightFactorManager) Stop() {
	close(m.stopChan)
}

func (m *WeightFactorManager) refreshLoop() {
	ticker := time.NewTicker(weightRefreshInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.updateWeights()
		case <-m.stopChan:
			return
		}
	}
}

func (m *WeightFactorManager) updateWeights() {
	m.updateLock.Lock()
	defer m.updateLock.Unlock()

	var wg sync.WaitGroup

	m.weights.Range(func(key, _ any) bool {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if weightFactor, err := fetchWeightFactor(m.client, addr); err != nil {
				m.RemoveAddress(addr)
			} else {
				weight := int32(weightFactor * defaultWeight)
				m.weights.Store(addr, weight)
			}
		}(key.(string))
		return true
	})

	wg.Wait()
}

func fetchWeightFactor(client *http.Client, addr string) (float64, error) {
	resp, err := client.Get("http://" + addr + weightFactorHTTPPath)
	if err != nil {
		return 1.0, err
	}

	body, err := io.ReadAll(resp.Body)
	defer func() {
		_ = resp.Body.Close()
	}()
	if err != nil {
		return 1.0, err
	}

	if weight, err := strconv.ParseFloat(string(body), 64); err == nil && weight > 0 {
		return weight, nil
	} else {
		return 1.0, err
	}
}

func GetWeightManager() *WeightFactorManager {
	once.Do(func() {
		globalWeightFactorManager = newWeightFactorManager()
		globalWeightFactorManager.start()
	})
	return globalWeightFactorManager
}
