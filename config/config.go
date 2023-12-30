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

	// LogFile configures the file output of the logger.
	LogFile struct {
		Enabled bool   `yaml:"enabled"`
		Level   string `yaml:"level"` // override the file log level
		Format  string `yaml:"format"`
		// Path is the path of the log file.
		Path string `yaml:"path"`
	}

	// StdOut configures the standard output of the logger.
	StdOut struct {
		Level      string `yaml:"level"` // override the stdout log level
		Enabled    bool   `yaml:"enabled"`
		Format     string `yaml:"format"`
		EnableANSI bool   `yaml:"enableANSI"` //nolint:tagliatelle
	}

	// Log contains the configuration for the logger.
	Log struct {
		// Path is the directory to store the hostd.log file.
		// Deprecated: use File.Path instead.
		Path   string  `yaml:"path"`
		Level  string  `yaml:"level"` // global log level
		StdOut StdOut  `yaml:"stdout"`
		File   LogFile `yaml:"file"`
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
