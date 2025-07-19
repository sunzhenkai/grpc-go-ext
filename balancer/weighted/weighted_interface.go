package weighted

type WeightFactorManagerIf interface {
	GetAddressWeight(endpoint string) int32
	RemoveAddress(addrs ...string)
}
