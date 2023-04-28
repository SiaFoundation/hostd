package build

//go:generate go run gen.go

// NetworkName returns the human-readable name of the current network.
func NetworkName() string {
	n, _ := Network()
	switch n.Name {
	case "mainnet":
		return "Mainnet"
	case "zen":
		return "Zen Testnet"
	default:
		return n.Name
	}
}
