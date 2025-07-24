package weighted

type WeightManagerIf interface {
	GetAddressWeight(endpoint string) int32
	RemoveAddress(addrs ...string)
}
