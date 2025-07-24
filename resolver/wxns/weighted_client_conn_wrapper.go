package wxns

import (
	"google.golang.org/grpc/attributes"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
)

type weightedClientConnWrapper struct {
	cc resolver.ClientConn
}

func (i *weightedClientConnWrapper) UpdateState(state resolver.State) error {
	var newAddrs []resolver.Address
	for _, addr := range state.Addresses {
		hostPort := addr.Addr
		weight := int64(fetchWeight(hostPort))

		// 将 target 和 weight 都放到 Attributes 中
		newAttr := addr.Attributes
		if newAttr == nil {
			newAttr = attributes.New("weight", weight)
		} else {
			newAttr = newAttr.WithValue("weight", weight)
		}

		newAddrs = append(newAddrs, resolver.Address{
			Addr:               addr.Addr,
			ServerName:         addr.ServerName,
			Attributes:         newAttr, // 修改为包含 target 的 newAttr
			BalancerAttributes: addr.BalancerAttributes,
			Metadata:           addr.Metadata,
		})
	}
	return i.cc.UpdateState(resolver.State{Addresses: newAddrs, Endpoints: state.Endpoints, ServiceConfig: state.ServiceConfig, Attributes: state.Attributes})
}

func (i *weightedClientConnWrapper) ReportError(err error) {
	i.cc.ReportError(err)
}

func (i *weightedClientConnWrapper) NewAddress(addrs []resolver.Address) {
	i.cc.NewAddress(addrs)
}

func (i *weightedClientConnWrapper) ParseServiceConfig(config string) *serviceconfig.ParseResult {
	return i.cc.ParseServiceConfig(config)
}
