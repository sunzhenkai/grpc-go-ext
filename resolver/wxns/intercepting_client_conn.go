package wxns

import (
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type interceptingClientConn struct {
	cc       resolver.ClientConn
	onUpdate func(resolver.State)
	resolver *wxnsResolver
}

func (i *interceptingClientConn) UpdateState(s resolver.State) error {
	if i.onUpdate != nil {
		i.onUpdate(s)
		return nil
	}
	return i.cc.UpdateState(s)
}

func (i *interceptingClientConn) ReportError(err error) {
	i.cc.ReportError(err)
}

func (i *interceptingClientConn) NewAddress(addrs []resolver.Address) {
	i.cc.NewAddress(addrs)
}

func (i *interceptingClientConn) ParseServiceConfig(config string) *serviceconfig.ParseResult {
	return i.cc.ParseServiceConfig(config)
}

type legacyClientConn interface {
	NewServiceConfig(string)
}

func (i *interceptingClientConn) NewServiceConfig(config string) {
	if cc, ok := i.cc.(legacyClientConn); ok {
		cc.NewServiceConfig(config)
	}
}
