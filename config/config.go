package config

import (
	"go.sia.tech/coreutils/chain"
)

type (
	// HTTP contains the configuration for the HTTP server.
	HTTP struct {
		Address  string `yaml:"address,omitempty"`
		Password string `yaml:"password,omitempty"`
	}

	// Syncer contains the configuration for the p2p syncer.
	Syncer struct {
		Address    string   `yaml:"address,omitempty"`
		Bootstrap  bool     `yaml:"bootstrap,omitempty"`
		EnableUPnP bool     `yaml:"enableUPnP,omitempty"`
		Peers      []string `yaml:"peers,omitempty"`
	}

	// Consensus contains the configuration for the consensus set.
	Consensus struct {
		Network        string `yaml:"network,omitempty"`
		IndexBatchSize int    `yaml:"indexBatchSize,omitempty"`
	}

	// ExplorerData contains the configuration for using an external explorer.
	ExplorerData struct {
		Disable bool   `yaml:"disable,omitempty"`
		URL     string `yaml:"url,omitempty"`
	}

	// RHP2 contains the configuration for the RHP2 server.
	RHP2 struct {
		Address string `yaml:"address,omitempty"`
	}

	// RHP3 contains the configuration for the RHP3 server.
	RHP3 struct {
		TCPAddress string `yaml:"tcp,omitempty"`
	}

	// RHP4ListenAddress contains the configuration for an RHP4 listen address.
	RHP4ListenAddress struct {
		Protocol string `yaml:"protocol,omitempty"`
		Address  string `yaml:"address,omitempty"`
	}

	// RHP4AnnounceAddress contains the configuration for an RHP4 announce address.
	RHP4AnnounceAddress struct {
		Protocol chain.Protocol `yaml:"protocol,omitempty"`
		Address  string         `yaml:"address,omitempty"`
	}

	// RHP4 contains the configuration for the RHP4 server.
	RHP4 struct {
		ListenAddresses   []RHP4ListenAddress   `yaml:"listenAddresses,omitempty"`
		AnnounceAddresses []RHP4AnnounceAddress `yaml:"announceAddresses,omitempty"`
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

	// Config contains the configuration for the host.
	Config struct {
		Name           string `yaml:"name,omitempty"`
		Directory      string `yaml:"directory,omitempty"`
		RecoveryPhrase string `yaml:"recoveryPhrase,omitempty"`
		AutoOpenWebUI  bool   `yaml:"autoOpenWebUI,omitempty"`

		HTTP      HTTP         `yaml:"http,omitempty"`
		Syncer    Syncer       `yaml:"syncer,omitempty"`
		Consensus Consensus    `yaml:"consensus,omitempty"`
		Explorer  ExplorerData `yaml:"explorer,omitempty"`
		RHP2      RHP2         `yaml:"rhp2,omitempty"`
		RHP3      RHP3         `yaml:"rhp3,omitempty"`
		RHP4      RHP4         `yaml:"rhp4,omitempty"`
		Log       Log          `yaml:"log,omitempty"`
	}
)
