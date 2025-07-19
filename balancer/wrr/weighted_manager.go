package wrr

import (
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type weightFactorManager struct {
	weights    sync.Map // map[string]int32
	total      int32
	client     *http.Client
	stopChan   chan struct{}
	updateLock sync.Mutex
}

func newWeightFactorManager() *weightFactorManager {
	return &weightFactorManager{
		client:   &http.Client{Timeout: 2 * time.Second},
		stopChan: make(chan struct{}),
	}
}

func (m *weightFactorManager) start() {
	go m.refreshLoop()
}

func (m *weightFactorManager) stop() {
	close(m.stopChan)
}

func (m *weightFactorManager) refreshLoop() {
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

func (m *weightFactorManager) updateWeights() {
	m.updateLock.Lock()
	defer m.updateLock.Unlock()

	var total int32
	var wg sync.WaitGroup

	m.weights.Range(func(key, _ interface{}) bool {
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			weight := fetchWeight(m.client, addr)
			m.weights.Store(addr, weight)
			atomic.AddInt32(&total, weight)
		}(key.(string))
		return true
	})

	wg.Wait()
	atomic.StoreInt32(&m.total, total)
}

func fetchWeight(client *http.Client, addr string) int32 {
	resp, err := client.Get("http://" + addr + weightHTTPPath)
	if err != nil {
		return 1
	}
	defer resp.Body.Close()

	var result struct {
		Weight int32 `json:"weight"`
	}
	if json.NewDecoder(resp.Body).Decode(&result) != nil {
		return 1
	}
	return max(1, result.Weight)
}

func GetWeightManager() *weightFactorManager {
	once.Do(func() {
		globalWeightFactorManager = newWeightFactorManager()
		globalWeightFactorManager.start()
	})
	return globalWeightFactorManager
}
