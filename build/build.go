package build

//go:generate go run gen.go

// Network returns the name of the network that the hostd binary is configured
// to run on.
func Network() string { return network }
