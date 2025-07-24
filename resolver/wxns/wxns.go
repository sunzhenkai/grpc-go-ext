package wxns

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/dns"
)

var ResolvingPeriod = 30 * time.Second

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

func (b *wxnsBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	// target.URL.Scheme = "dns"
	// ncc := &weightedClientConnWrapper{cc: cc}
	dr, err := dns.NewBuilder().Build(target, cc, opts)
	// dr, error := resolver.Get("dns").Build(target, ncc, opts)
	if err != nil {
		return dr, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &wxnsResolver{
		dnsResolver: dr,
		ctx:         ctx,
		cancel:      cancel,
	}
	d.wg.Add(1)
	go d.refresh()
	return d, nil
}

type wxnsResolver struct {
	dnsResolver resolver.Resolver
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
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
