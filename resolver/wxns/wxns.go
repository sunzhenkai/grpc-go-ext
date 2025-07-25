package wxns

import (
	"context"
	"log"
	"reflect"
	"sort"
	"sync"
	"time"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/dns"
)

var ResolvingPeriod = 60 * time.Second

func init() {
	resolver.Register(NewBuilder())
	log.Printf("wxns resolver registered with scheme: %s", NewBuilder().Scheme())
}

func NewBuilder() resolver.Builder {
	return &wxnsBuilder{}
}

type wxnsBuilder struct{}

func (b *wxnsBuilder) Scheme() string {
	return "wxns"
}

type wxnsResolver struct {
	dnsResolver resolver.Resolver
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	cc         resolver.ClientConn
	lastAddrs  []resolver.Address
	stateMutex sync.Mutex
}

func (b *wxnsBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	icc := &interceptingClientConn{
		cc:       cc,
		onUpdate: nil,
		resolver: nil,
	}
	dr, err := dns.NewBuilder().Build(target, icc, opts)
	// dr, error := resolver.Get("dns").Build(target, ncc, opts)
	if err != nil {
		return dr, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &wxnsResolver{
		dnsResolver: dr,
		ctx:         ctx,
		cancel:      cancel,
		cc:          cc,
		lastAddrs:   nil,
	}
	icc.onUpdate = d.handleUpdateState
	icc.resolver = d
	d.wg.Add(1)
	go d.refresh()
	return d, nil
}

func (r *wxnsResolver) handleUpdateState(s resolver.State) {
	r.stateMutex.Lock()
	defer r.stateMutex.Unlock()

	newAddrs := normalizeAddresses(s.Addresses)
	if reflect.DeepEqual(r.lastAddrs, newAddrs) {
		log.Println("wxnsResolver: resolved addresses unchanged, skip update")
		return
	}

	log.Printf("wxnsResolver: resolved addresses updated: %+v\n", newAddrs)
	r.lastAddrs = newAddrs
	err := r.cc.UpdateState(resolver.State{Addresses: newAddrs})
	if err != nil {
		log.Printf("wxnsResolver: cc.UpdateState error: %v\n", err)
	}
}

func normalizeAddresses(addrs []resolver.Address) []resolver.Address {
	sorted := make([]resolver.Address, len(addrs))
	copy(sorted, addrs)
	sort.SliceStable(sorted, func(i, j int) bool {
		return sorted[i].Addr < sorted[j].Addr
	})
	return sorted
}

func (b *wxnsResolver) refresh() {
	defer b.wg.Done()
	ticker := time.NewTicker(ResolvingPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			log.Println("wxnsResolver: refresh stopped due to context cancel")
			return
		case <-ticker.C:
			log.Println("wxnsResolver: periodic refresh triggered")
			b.dnsResolver.ResolveNow(resolver.ResolveNowOptions{})
		}
	}
}

func (b *wxnsResolver) ResolveNow(opts resolver.ResolveNowOptions) {
	b.dnsResolver.ResolveNow(opts)
}

func (d *wxnsResolver) Close() {
	d.cancel()
	d.wg.Wait()
	d.dnsResolver.Close()
}
