package types

type Network string

const (
	NetworkEthMainnet Network = "eth"
	NetworkEthSepolia Network = "sepolia"
	NetworkBscMainnet Network = "bsc"
)

func (n Network) String() string {
	return string(n)
}
