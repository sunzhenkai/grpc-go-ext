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
	Name                  = "weighted_round_robin_http"
	weightHTTPPath        = "/rpc/weight"
	defaultWeight         = 1000
	weightRefreshInterval = 5 * time.Second
)

func init() {
	balancer.Register(newBuilder())
}

func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Name, &weightedPickerBuilder{}, base.Config{HealthCheck: true})
}

// subConnWeightInfo 存储单个 SubConn 的权重和 HTTP 地址
type subConnWeightInfo struct {
	addr      string       // HTTP 权重接口地址 (e.g., "localhost:50052")
	weight    int          // 当前权重
	lastCheck time.Time    // 上次检查时间
	mu        sync.RWMutex // 保护该 SubConn 的权重
}

// weightManager 管理所有 SubConn 的权重和异步刷新
type weightManager struct {
	sync.RWMutex
	subConnMap map[balancer.SubConn]*subConnWeightInfo
}

var (
	globalWeightManager *weightManager
	once                sync.Once
)

func GetWeightManager() *weightManager {
	once.Do(func() {
		globalWeightManager = &weightManager{
			subConnMap: make(map[balancer.SubConn]*subConnWeightInfo),
		}
		go globalWeightManager.startWeightRefresher()
	})
	return globalWeightManager
}

func (wm *weightManager) startWeightRefresher() {
	ticker := time.NewTicker(weightRefreshInterval)
	defer ticker.Stop()

	for range ticker.C {
		wm.RLock()
		currentSCs := make(map[balancer.SubConn]*subConnWeightInfo)
		for sc, info := range wm.subConnMap {
			currentSCs[sc] = info
		}
		wm.RUnlock()

		var wg sync.WaitGroup
		for sc, info := range currentSCs {
			wg.Add(1)
			go func(sc balancer.SubConn, info *subConnWeightInfo) {
				defer wg.Done()
				weight, err := getWeightFromHTTPTarget(info.addr)
				info.mu.Lock()
				defer info.mu.Unlock()
				if err != nil {
					log.Printf("Failed to refresh weight for SubConn %s (HTTP at %s): %v. Keeping previous weight %d.",
						sc.Address().Addr, info.addr, err, info.weight)
				} else {
					info.weight = weight
				}
				info.lastCheck = time.Now()
			}(sc, info)
		}
		wg.Wait()
	}
}

func (wm *weightManager) UpdateSubConns(readySCs map[balancer.SubConn]balancer.SubConnState) {
	wm.Lock()
	defer wm.Unlock()

	for sc := range wm.subConnMap {
		if _, ok := readySCs[sc]; !ok {
			delete(wm.subConnMap, sc)
			log.Printf("Removed inactive SubConn from weight manager: %s", sc.Address().Addr)
		}
	}

	for sc, scInfo := range readySCs {
		if _, ok := wm.subConnMap[sc]; !ok {
			gRPCHost, gRPCPortStr, err := net.SplitHostPort(scInfo.Address.Addr)
			if err != nil {
				log.Printf("WeightedBalancer: Invalid gRPC address format for %s: %v. Cannot add to manager.", scInfo.Address.Addr, err)
				continue
			}
			gRPCPort, err := strconv.Atoi(gRPCPortStr)
			if err != nil {
				log.Printf("WeightedBalancer: Invalid gRPC port format for %s: %v. Cannot add to manager.", scInfo.Address.Addr, err)
				continue
			}
			httpPort := gRPCPort + 1
			httpAddr := net.JoinHostPort(gRPCHost, strconv.Itoa(httpPort))

			wm.subConnMap[sc] = &subConnWeightInfo{
				addr:      httpAddr,
				weight:    defaultWeight,
				lastCheck: time.Now(),
			}
			log.Printf("Added new SubConn %s (HTTP: %s) to weight manager with default weight %d.", scInfo.Address.Addr, httpAddr, defaultWeight)

			go func(sc balancer.SubConn, info *subConnWeightInfo) {
				weight, err := getWeightFromHTTPTarget(info.addr)
				info.mu.Lock()
				defer info.mu.Unlock()
				if err != nil {
					log.Printf("Initial weight fetch failed for SubConn %s (HTTP at %s): %v. Using default %d.",
						sc.Address().Addr, info.addr, err, info.weight)
				} else {
					info.weight = weight
				}
			}(sc, wm.subConnMap[sc])
		}
	}
}

func (wm *weightManager) GetWeight(sc balancer.SubConn) int {
	wm.RLock()
	info, ok := wm.subConnMap[sc]
	wm.RUnlock()

	if !ok {
		log.Printf("WeightedBalancer: SubConn %s not found in manager. Using default weight %d.", sc.Address().Addr, defaultWeight)
		return defaultWeight
	}

	info.mu.RLock()
	defer info.mu.RUnlock()
	return info.weight
}

func getWeightFromHTTPTarget(httpAddr string) (int, error) {
	client := http.Client{
		Timeout: 2 * time.Second,
	}
	resp, err := client.Get(fmt.Sprintf("http://%s%s", httpAddr, weightHTTPPath))
	if err != nil {
		return 0, fmt.Errorf("could not reach HTTP weight endpoint at %s%s: %v", httpAddr, weightHTTPPath, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("HTTP status error from %s%s: %s", httpAddr, weightHTTPPath, resp.Status)
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("failed to read response body from %s%s: %v", httpAddr, weightHTTPPath, err)
	}

	weight, err := strconv.Atoi(string(bodyBytes))
	if err != nil {
		return 0, fmt.Errorf("failed to parse weight '%s' from %s%s: %v", string(bodyBytes), httpAddr, weightHTTPPath, err)
	}
	if weight <= 0 {
		return defaultWeight, fmt.Errorf("received non-positive weight %d from %s, using default", weight, httpAddr)
	}
	return weight, nil
}

// ----------------------------------------------------------------------
// 优化后的 Picker 实现
// ----------------------------------------------------------------------

// weightedSubConnEntry 用于存储 SubConn 及其权重，以及累积权重上限
type weightedSubConnEntry struct {
	sc               balancer.SubConn
	weight           int
	cumulativeWeight int // 累积权重，用于快速查找 (关键)
}

// weightedPicker 实现了 balancer.Picker
type weightedPicker struct {
	// subConnsAndWeights 存储所有 SubConn 及其权重，以及用于加权选择的累积权重列表。
	// 这是一个在 Build 阶段就构建好的**不可变快照**。
	subConnsAndWeights []weightedSubConnEntry
	r                  *rand.Rand // 随机数生成器。需要保护。
	rmu                sync.Mutex // 保护随机数生成器
}

type weightedPickerBuilder struct{}

// weightedPickerBuilder 实现了 base.PickerBuilder
// Build 方法中构建 weightedPicker
func (*weightedPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	log.Printf("WeightedBalancer: Building new Picker. Ready SubConns: %d", len(info.ReadySCs))
	if len(info.ReadySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	manager := GetWeightManager()
	manager.UpdateSubConns(info.ReadySCs) // 通知管理器更新当前的SubConn集合

	// 1. 获取所有 SubConn 及其当前权重，并预计算累积权重
	var scEntries []weightedSubConnEntry
	totalWeight := 0
	for sc := range info.ReadySCs {
		weight := manager.GetWeight(sc) // 从异步更新的管理器获取权重
		if weight <= 0 {                // 确保权重有效，避免负权重或零权重导致问题
			weight = defaultWeight
		}
		totalWeight += weight
		scEntries = append(scEntries, weightedSubConnEntry{
			sc:               sc,
			weight:           weight,
			cumulativeWeight: totalWeight, // 累积权重
		})
	}

	if len(scEntries) == 0 || totalWeight == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	return &weightedPicker{
		subConnsAndWeights: scEntries,                                       // Picker 持有预计算好的数据 (不可变)
		r:                  rand.New(rand.NewSource(time.Now().UnixNano())), // 每个 Picker 实例有自己的随机数生成器
		// rmu:                sync.Mutex{}, // 保护随机数生成器
	}
}

// Pick 实现负载均衡的逻辑 - 优化后
func (p *weightedPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.PickResult, error) {
	// 1. 保护随机数生成器
	p.rmu.Lock()
	// 生成一个 [0, totalWeight-1] 之间的随机数
	// totalWeight 存储在最后一个 entry 的 cumulativeWeight 中
	randVal := p.r.Intn(p.subConnsAndWeights[len(p.subConnsAndWeights)-1].cumulativeWeight)
	p.rmu.Unlock()

	// 2. 使用二分查找 (Binary Search) 查找命中节点
	// 因为 cumulativeWeight 是递增的，所以可以使用二分查找
	// Go 的 sort.SearchInts 适用于有序 int 切片，这里需要手动实现
	// 或者使用 sort.Search 配合自定义比较函数

	// 查找第一个 cumulativeWeight 大于 randVal 的索引
	idx := search(len(p.subConnsAndWeights), func(i int) bool {
		return p.subConnsAndWeights[i].cumulativeWeight > randVal
	})

	// 理论上 idx 应该总是在有效范围内
	if idx < 0 || idx >= len(p.subConnsAndWeights) {
		// 出现这种情况通常是逻辑错误，或者 randVal == totalWeight （不应该发生）
		log.Printf("WeightedBalancer: Fallback to random pick due to binary search failure. RandVal: %d, Entries: %v", randVal, p.subConnsAndWeights)
		// 兜底方案：随机选择一个
		p.rmu.Lock()
		fallbackIdx := p.r.Intn(len(p.subConnsAndWeights))
		p.rmu.Unlock()
		return balancer.PickResult{SubConn: p.subConnsAndWeights[fallbackIdx].sc}, nil
	}

	return balancer.PickResult{SubConn: p.subConnsAndWeights[idx].sc}, nil
}

// search 是 sort.Search 的简化版本，用于二分查找
// 查找满足 f(i) 为 true 的最小 i
func search(n int, f func(int) bool) int {
	// Define f(-1) == false and f(n) == true.
	// Invariant: f(i-1) == false, f(j) == true.
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when i+j is large
		if !f(h) {
			i = h + 1 // f(h) is false, so f(i-1) is still false
		} else {
			j = h // f(h) is true, so f(j) is still true
		}
	}
	// i == j, f(i-1) == false, and f(i) == true.
	return i
}
