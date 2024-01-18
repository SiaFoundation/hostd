package config

type (
	// HTTP contains the configuration for the HTTP server.
	HTTP struct {
		Address  string `yaml:"address,omitempty"`
		Password string `yaml:"password,omitempty"`
	}

	// Consensus contains the configuration for the consensus set.
	Consensus struct {
		GatewayAddress string   `yaml:"gatewayAddress,omitempty"`
		Bootstrap      bool     `yaml:"bootstrap,omitempty"`
		Peers          []string `yaml:"peers,omitempty"`
	}

	// RHP2 contains the configuration for the RHP2 server.
	RHP2 struct {
		Address string `yaml:"address,omitempty"`
	}

	// RHP3 contains the configuration for the RHP3 server.
	RHP3 struct {
		TCPAddress       string `yaml:"tcp,omitempty"`
		WebSocketAddress string `yaml:"websocket,omitempty"`
		CertPath         string `yaml:"certPath,omitempty"`
		KeyPath          string `yaml:"keyPath,omitempty"`
	}

	// LogFile configures the file output of the logger.
	LogFile struct {
		Enabled bool   `yaml:"enabled,omitempty"`
		Level   string `yaml:"level,omitempty"` // override the file log level
		Format  string `yaml:"format,omitempty"`
		// Path is the path of the log file.
		Path string `yaml:"path,omitempty"`
	}

	// StdOut configures the standard output of the logger.
	StdOut struct {
		Level      string `yaml:"level,omitempty"` // override the stdout log level
		Enabled    bool   `yaml:"enabled,omitempty"`
		Format     string `yaml:"format,omitempty"`
		EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
	}

	// Log contains the configuration for the logger.
	Log struct {
		// Path is the directory to store the hostd.log file.
		// Deprecated: use File.Path instead.
		Path   string  `yaml:"path,omitempty"`
		Level  string  `yaml:"level,omitempty"` // global log level
		StdOut StdOut  `yaml:"stdout,omitempty"`
		File   LogFile `yaml:"file,omitempty"`
	}

	// Prometheus contains the configuration for exporting Prometheus metrics
	Prometheus struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
	}

	// Config contains the configuration for the host.
	Config struct {
		Name           string `yaml:"name,omitempty"`
		Directory      string `yaml:"directory,omitempty"`
		RecoveryPhrase string `yaml:"recoveryPhrase,omitempty"`
		AutoOpenWebUI  bool   `yaml:"autoOpenWebUI,omitempty"`

		HTTP       HTTP       `yaml:"http,omitempty"`
		Consensus  Consensus  `yaml:"consensus,omitempty"`
		RHP2       RHP2       `yaml:"rhp2,omitempty"`
		RHP3       RHP3       `yaml:"rhp3,omitempty"`
		Log        Log        `yaml:"log,omitempty"`
		Prometheus Prometheus `yaml:"prometheus,omitempty"`
	}
)
