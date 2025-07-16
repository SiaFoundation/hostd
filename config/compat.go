package config

import (
	"fmt"
	"io"
	"path/filepath"
	"runtime"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type configV235 struct {
	Name           string `yaml:"name,omitempty"`
	Directory      string `yaml:"directory,omitempty"`
	RecoveryPhrase string `yaml:"recoveryPhrase,omitempty"`
	AutoOpenWebUI  bool   `yaml:"autoOpenWebUI,omitempty"`

	HTTP struct {
		Address  string `yaml:"address,omitempty"`
		Password string `yaml:"password,omitempty"`
	} `yaml:"http,omitempty"`
	Syncer struct {
		Address    string   `yaml:"address,omitempty"`
		Bootstrap  bool     `yaml:"bootstrap,omitempty"`
		EnableUPnP bool     `yaml:"enableUPnP,omitempty"`
		Peers      []string `yaml:"peers,omitempty"`
	} `yaml:"syncer,omitempty"`
	Consensus struct {
		Network        string `yaml:"network,omitempty"`
		IndexBatchSize int    `yaml:"indexBatchSize,omitempty"`
	} `yaml:"consensus,omitempty"`
	Explorer struct {
		Disable bool   `yaml:"disable,omitempty"`
		URL     string `yaml:"url,omitempty"`
	} `yaml:"explorer,omitempty"`
	Log struct {
		Level  string `yaml:"level,omitempty"` // global log level
		StdOut struct {
			Level      string `yaml:"level,omitempty"` // override the stdout log level
			Enabled    bool   `yaml:"enabled,omitempty"`
			Format     string `yaml:"format,omitempty"`
			EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
		} `yaml:"stdout,omitempty"`
		File struct {
			Enabled bool   `yaml:"enabled,omitempty"`
			Level   string `yaml:"level,omitempty"` // override the file log level
			Format  string `yaml:"format,omitempty"`
			Path    string `yaml:"path,omitempty"`
		} `yaml:"file,omitempty"`
	}
	RHP2 struct {
		Address string `yaml:"address,omitempty"`
	} `yaml:"rhp2,omitempty"`
	RHP3 struct {
		TCPAddress string `yaml:"tcp,omitempty"`
	} `yaml:"rhp3,omitempty"`
	RHP4 struct {
		QUIC struct {
			CertPath string `yaml:"certPath,omitempty"`
			KeyPath  string `yaml:"keyPath,omitempty"`
		} `yaml:"quic,omitempty"`
		ListenAddresses []struct {
			Protocol string `yaml:"protocol,omitempty"`
			Address  string `yaml:"address,omitempty"`
		} `yaml:"listenAddresses,omitempty"`
	} `yaml:"rhp4,omitempty"`
}

type configV112 struct {
	Name           string `yaml:"name,omitempty"`
	Directory      string `yaml:"directory,omitempty"`
	RecoveryPhrase string `yaml:"recoveryPhrase,omitempty"`
	AutoOpenWebUI  bool   `yaml:"autoOpenWebUI,omitempty"`

	HTTP struct {
		Address  string `yaml:"address,omitempty"`
		Password string `yaml:"password,omitempty"`
	} `yaml:"http,omitempty"`
	Consensus struct {
		GatewayAddress string   `yaml:"gatewayAddress,omitempty"`
		Bootstrap      bool     `yaml:"bootstrap,omitempty"`
		Peers          []string `yaml:"peers,omitempty"`
	} `yaml:"consensus,omitempty"`
	Explorer struct {
		Disable bool   `yaml:"disable,omitempty"`
		URL     string `yaml:"url,omitempty"`
	} `yaml:"explorer,omitempty"`
	RHP2 struct {
		Address string `yaml:"address,omitempty"`
	} `yaml:"rhp2,omitempty"`
	RHP3 struct {
		TCPAddress       string `yaml:"tcp,omitempty"`
		WebSocketAddress string `yaml:"websocket,omitempty"`
		CertPath         string `yaml:"certPath,omitempty"`
		KeyPath          string `yaml:"keyPath,omitempty"`
	} `yaml:"rhp3,omitempty"`
	Log struct {
		// Path is the directory to store the hostd.log file.
		// Deprecated: use File.Path instead.
		Path   string `yaml:"path,omitempty"`
		Level  string `yaml:"level,omitempty"` // global log level
		StdOut struct {
			Level      string `yaml:"level,omitempty"` // override the stdout log level
			Enabled    bool   `yaml:"enabled,omitempty"`
			Format     string `yaml:"format,omitempty"`
			EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
		} `yaml:"stdout,omitempty"`
		File struct {
			Enabled bool   `yaml:"enabled,omitempty"`
			Level   string `yaml:"level,omitempty"` // override the file log level
			Format  string `yaml:"format,omitempty"`
			// Path is the path of the log file.
			Path string `yaml:"path,omitempty"`
		} `yaml:"file,omitempty"`
	} `yaml:"log,omitempty"`
}

// updateConfigV235 updates the config file from v2.3.5 to the latest version.
// It returns an error if the config file cannot be updated.
func updateConfigV235(fp string, r io.Reader, cfg *Config) error {
	old := configV235{
		RHP4: struct {
			QUIC struct {
				CertPath string `yaml:"certPath,omitempty"`
				KeyPath  string `yaml:"keyPath,omitempty"`
			} `yaml:"quic,omitempty"`
			ListenAddresses []struct {
				Protocol string `yaml:"protocol,omitempty"`
				Address  string `yaml:"address,omitempty"`
			} `yaml:"listenAddresses,omitempty"`
		}{
			ListenAddresses: []struct {
				Protocol string `yaml:"protocol,omitempty"`
				Address  string `yaml:"address,omitempty"`
			}{
				{
					Protocol: "tcp",
					Address:  ":9984",
				},
				{
					Protocol: "quic",
					Address:  ":9984",
				},
			},
		},
		Log: struct {
			Level  string `yaml:"level,omitempty"` // global log level
			StdOut struct {
				Level      string `yaml:"level,omitempty"` // override the stdout log level
				Enabled    bool   `yaml:"enabled,omitempty"`
				Format     string `yaml:"format,omitempty"`
				EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
			} `yaml:"stdout,omitempty"`
			File struct {
				Enabled bool   `yaml:"enabled,omitempty"`
				Level   string `yaml:"level,omitempty"` // override the file log level
				Format  string `yaml:"format,omitempty"`
				Path    string `yaml:"path,omitempty"`
			} `yaml:"file,omitempty"`
		}{
			Level: "info",
			StdOut: struct {
				Level      string `yaml:"level,omitempty"` // override the stdout log level
				Enabled    bool   `yaml:"enabled,omitempty"`
				Format     string `yaml:"format,omitempty"`
				EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
			}{
				Enabled:    true,
				Format:     "human",
				EnableANSI: runtime.GOOS != "windows",
			},
		},
	}
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&old); err != nil {
		return fmt.Errorf("failed to decode config file: %w", err)
	}

	cfg.Name = old.Name
	cfg.Directory = old.Directory
	cfg.RecoveryPhrase = old.RecoveryPhrase
	cfg.AutoOpenWebUI = old.AutoOpenWebUI
	cfg.HTTP = old.HTTP
	cfg.Syncer = old.Syncer
	cfg.Consensus = old.Consensus
	cfg.Explorer = old.Explorer
	cfg.RHP4.QUIC = old.RHP4.QUIC
	if len(old.RHP4.ListenAddresses) > 0 {
		cfg.RHP4.ListenAddresses = make([]RHP4ListenAddress, 0, len(old.RHP4.ListenAddresses))
		for _, addr := range old.RHP4.ListenAddresses {
			cfg.RHP4.ListenAddresses = append(cfg.RHP4.ListenAddresses, RHP4ListenAddress{
				Protocol: RHP4Proto(addr.Protocol),
				Address:  addr.Address,
			})
		}
	}

	var level zap.AtomicLevel
	if err := level.UnmarshalText([]byte(old.Log.Level)); err != nil {
		return fmt.Errorf("failed to parse log level: %w", err)
	}
	if old.Log.File.Level == "" {
		cfg.Log.File.Level = level
	} else if err := cfg.Log.File.Level.UnmarshalText([]byte(old.Log.File.Level)); err != nil {
		return fmt.Errorf("failed to parse file log level: %w", err)
	}
	cfg.Log.File.Path = old.Log.File.Path
	cfg.Log.File.Enabled = old.Log.File.Enabled
	cfg.Log.File.Format = old.Log.File.Format

	if old.Log.StdOut.Level == "" {
		cfg.Log.StdOut.Level = level
	} else if err := cfg.Log.StdOut.Level.UnmarshalText([]byte(old.Log.StdOut.Level)); err != nil {
		return fmt.Errorf("failed to parse stdout log level: %w", err)
	}
	cfg.Log.StdOut.Enabled = old.Log.StdOut.Enabled
	cfg.Log.StdOut.Format = old.Log.StdOut.Format
	cfg.Log.StdOut.EnableANSI = old.Log.StdOut.EnableANSI

	return nil
}

// updateConfigV112 updates the config file from v1.1.2 to the latest version.
// It returns an error if the config file cannot be updated.
func updateConfigV112(fp string, r io.Reader, cfg *Config) error {
	old := configV112{
		Consensus: struct {
			GatewayAddress string   `yaml:"gatewayAddress,omitempty"`
			Bootstrap      bool     `yaml:"bootstrap,omitempty"`
			Peers          []string `yaml:"peers,omitempty"`
		}{
			GatewayAddress: ":9981",
			Bootstrap:      true,
		},
		RHP2: struct {
			Address string `yaml:"address,omitempty"`
		}{
			Address: ":9982",
		},
		RHP3: struct {
			TCPAddress       string `yaml:"tcp,omitempty"`
			WebSocketAddress string `yaml:"websocket,omitempty"`
			CertPath         string `yaml:"certPath,omitempty"`
			KeyPath          string `yaml:"keyPath,omitempty"`
		}{
			TCPAddress: ":9983",
		},
		Log: struct {
			Path   string `yaml:"path,omitempty"`
			Level  string `yaml:"level,omitempty"`
			StdOut struct {
				Level      string `yaml:"level,omitempty"` // override the stdout log level
				Enabled    bool   `yaml:"enabled,omitempty"`
				Format     string `yaml:"format,omitempty"`
				EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
			} `yaml:"stdout,omitempty"`
			File struct {
				Enabled bool   `yaml:"enabled,omitempty"`
				Level   string `yaml:"level,omitempty"` // override the file log level
				Format  string `yaml:"format,omitempty"`
				// Path is the path of the log file.
				Path string `yaml:"path,omitempty"`
			} `yaml:"file,omitempty"`
		}{
			Level: "info",
			StdOut: struct {
				Level      string `yaml:"level,omitempty"`
				Enabled    bool   `yaml:"enabled,omitempty"`
				Format     string `yaml:"format,omitempty"`
				EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
			}{
				Level:   "info",
				Enabled: true,
				Format:  "human",
			},
			File: struct {
				Enabled bool   `yaml:"enabled,omitempty"`
				Level   string `yaml:"level,omitempty"` // override the file log level
				Format  string `yaml:"format,omitempty"`
				// Path is the path of the log file.
				Path string `yaml:"path,omitempty"`
			}{
				Enabled: true,
				Level:   "info",
				Format:  "json",
			},
		},
	}
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)
	if err := dec.Decode(&old); err != nil {
		return fmt.Errorf("failed to decode config file: %w", err)
	}

	cfg.Name = old.Name
	cfg.Directory = old.Directory
	cfg.RecoveryPhrase = old.RecoveryPhrase
	cfg.AutoOpenWebUI = old.AutoOpenWebUI
	cfg.HTTP.Address = old.HTTP.Address
	cfg.HTTP.Password = old.HTTP.Password
	cfg.Syncer.Address = old.Consensus.GatewayAddress
	cfg.Syncer.Bootstrap = old.Consensus.Bootstrap
	cfg.Syncer.Peers = old.Consensus.Peers
	cfg.Explorer.Disable = old.Explorer.Disable
	cfg.Explorer.URL = old.Explorer.URL
	if old.Log.File.Path != "" {
		cfg.Log.File.Path = old.Log.File.Path
	} else if old.Log.Path != "" {
		cfg.Log.File.Path = filepath.Join(old.Log.Path, "hostd.log")
	}
	if old.Log.Level == "" {
		old.Log.Level = "info"
	}
	var level zap.AtomicLevel
	if err := level.UnmarshalText([]byte(old.Log.Level)); err != nil {
		return fmt.Errorf("failed to parse log level: %w", err)
	}
	if old.Log.File.Level == "" {
		cfg.Log.File.Level = level
	} else if err := cfg.Log.File.Level.UnmarshalText([]byte(old.Log.File.Level)); err != nil {
		return fmt.Errorf("failed to parse file log level: %w", err)
	}
	cfg.Log.File.Enabled = old.Log.File.Enabled
	cfg.Log.File.Format = old.Log.File.Format

	if old.Log.StdOut.Level == "" {
		cfg.Log.StdOut.Level = level
	} else if err := cfg.Log.StdOut.Level.UnmarshalText([]byte(old.Log.StdOut.Level)); err != nil {
		return fmt.Errorf("failed to parse stdout log level: %w", err)
	}
	cfg.Log.StdOut.Enabled = old.Log.StdOut.Enabled
	cfg.Log.StdOut.Format = old.Log.StdOut.Format
	cfg.Log.StdOut.EnableANSI = old.Log.StdOut.EnableANSI

	return nil
}
