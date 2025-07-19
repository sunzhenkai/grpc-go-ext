package xns

import (
	"context"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/dns"
)

var ResolvingPeriod = 30 * time.Second

func NewBuilder() resolver.Builder {
	return &xnsBuilder{}
}

type xnsBuilder struct{}

func (b *xnsBuilder) Scheme() string {
	return "xns"
}

func (b *xnsBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	dr, err := dns.NewBuilder().Build(target, cc, opts)
	// dr, error := resolver.Get("dns").Build(target, cc, opts)
	if err != nil {
		return dr, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	d := &xnsResolver{
		dnsResolver: dr,
		ctx:         ctx,
		cancel:      cancel,
	}
	d.wg.Add(1)
	go d.refresh()
	return d, nil
}

type xnsResolver struct {
	dnsResolver resolver.Resolver
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

func (b *xnsResolver) refresh() {
	defer b.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-b.ctx.Done():
			log.Println("xnsResolver: refresh stopped due to context cancel")
			return
		case <-ticker.C:
			log.Println("xnsResolver: periodic refresh triggered")
			b.dnsResolver.ResolveNow(resolver.ResolveNowOptions{})
		}
	}
}

func (b *xnsResolver) ResolveNow(opts resolver.ResolveNowOptions) {
	b.dnsResolver.ResolveNow(opts)
}

func (d *xnsResolver) Close() {
	d.cancel()
	d.wg.Wait()
	d.dnsResolver.Close()
}
