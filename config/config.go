package config

type (
	HTTP struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
	}

	Consensus struct {
		GatewayAddress string   `yaml:"gatewayAddress"`
		Bootstrap      bool     `yaml:"bootstrap"`
		Peers          []string `toml:"peers,omitempty"`
	}

	RHP2 struct {
		Address string `yaml:"address"`
	}

	RHP3 struct {
		TCPAddress       string `yaml:"tcp"`
		WebSocketAddress string `yaml:"websocket"`
		CertPath         string `yaml:"certPath"`
		KeyPath          string `yaml:"keyPath"`
	}

	Log struct {
		Path  string `yaml:"path"`
		Level string `yaml:"level"`
	}

	Config struct {
		Name           string `yaml:"name"`
		Directory      string `yaml:"directory"`
		RecoveryPhrase string `yaml:"recoveryPhrase"`

		HTTP      HTTP      `yaml:"http"`
		Consensus Consensus `yaml:"consensus"`
		RHP2      RHP2      `yaml:"rhp2"`
		RHP3      RHP3      `yaml:"rhp3"`
		Log       Log       `yaml:"log"`
	}
)
