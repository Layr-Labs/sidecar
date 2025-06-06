package providers

type InterestingContractsProvider interface {
	ListInterestingContractAddresses() ([]string, error)
}
