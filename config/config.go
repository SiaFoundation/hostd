package config

type (
	// HTTP contains the configuration for the HTTP server.
	HTTP struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
	}

	// Consensus contains the configuration for the consensus set.
	Consensus struct {
		GatewayAddress string   `yaml:"gatewayAddress"`
		Bootstrap      bool     `yaml:"bootstrap"`
		Peers          []string `toml:"peers,omitempty"`
	}

	// RHP2 contains the configuration for the RHP2 server.
	RHP2 struct {
		Address string `yaml:"address"`
	}

	// RHP3 contains the configuration for the RHP3 server.
	RHP3 struct {
		TCPAddress       string `yaml:"tcp"`
		WebSocketAddress string `yaml:"websocket"`
		CertPath         string `yaml:"certPath"`
		KeyPath          string `yaml:"keyPath"`
	}

	// Log contains the configuration for the logger.
	Log struct {
		Path  string `yaml:"path"`
		Level string `yaml:"level"`
	}

	// Config contains the configuration for the host.
	Config struct {
		Name           string `yaml:"name"`
		Directory      string `yaml:"directory"`
		RecoveryPhrase string `yaml:"recoveryPhrase"`
		AutoOpenWebUI  bool   `yaml:"autoOpenWebUI"`

		HTTP      HTTP      `yaml:"http"`
		Consensus Consensus `yaml:"consensus"`
		RHP2      RHP2      `yaml:"rhp2"`
		RHP3      RHP3      `yaml:"rhp3"`
		Log       Log       `yaml:"log"`
	}
)
