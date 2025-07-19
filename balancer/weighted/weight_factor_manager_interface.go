package weighted

type WeightFactorManagerIf interface {
	GetWeightFactor(endpoint string) float64
}
