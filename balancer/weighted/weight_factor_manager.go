package weighted

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

var (
	globalWeightManager   *WeightManager
	once                  sync.Once
	weightHTTPPath        = "/rpc/meta"
	weightRefreshInterval = 60 * time.Second
	defaultWeight         = 100
)

type WeightManager struct {
	weights    sync.Map // map[string]int32
	client     *http.Client
	stopChan   chan struct{}
	updateLock sync.Mutex
}

func newWeightManager() *WeightManager {
	return &WeightManager{
		client:   &http.Client{Timeout: 1 * time.Second},
		stopChan: make(chan struct{}),
	}
}

func (m *WeightManager) GetAddressWeight(endpoint string) int32 {
	if weight, ok := m.weights.Load(endpoint); ok {
		return weight.(int32)
	}

	if weight, err := fetchWeight(m.client, endpoint); err == nil {
		weight := int32(weight)
		m.weights.Store(endpoint, weight)
		return weight
	} else {
		return int32(defaultWeight)
	}
}

func (m *WeightManager) RemoveAddress(addrs ...string) {
	for _, addr := range addrs {
		if _, ok := m.weights.Load(addr); ok {
			m.weights.Delete(addr)
		}
	}
}

func (m *WeightManager) start() {
	go m.refreshLoop()
}

func (m *WeightManager) Stop() {
	close(m.stopChan)
}

func (m *WeightManager) refreshLoop() {
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

func (m *WeightManager) updateWeights() {
	m.updateLock.Lock()
	defer m.updateLock.Unlock()

	var wg sync.WaitGroup

	m.weights.Range(func(key, _ any) bool {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			if weight, err := fetchWeight(m.client, addr); err != nil {
				m.RemoveAddress(addr)
			} else {
				weight := int32(weight)
				m.weights.Store(addr, weight)
			}
		}(key.(string))
		return true
	})

	wg.Wait()
}

func fetchWeight(client *http.Client, addr string) (int, error) {
	resp, err := client.Get("http://" + addr + weightHTTPPath)
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

	var result struct {
		Weight int `json:"weight"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return 1.0, err
	}

	return result.Weight, nil
}

func GetWeightManager() *WeightManager {
	once.Do(func() {
		globalWeightManager = newWeightManager()
		globalWeightManager.start()
	})
	return globalWeightManager
}
