package config

import (
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

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

// updateConfigV112 updates the config file from v1.1.2 to the latest version.
// It returns an error if the config file cannot be updated.
func updateConfigV112(fp string, r io.Reader, cfg *Config) error {
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

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
	cfg.RHP2.Address = old.RHP2.Address
	cfg.RHP3.TCPAddress = old.RHP3.TCPAddress
	cfg.Log.Level = old.Log.Level
	if old.Log.File.Path != "" {
		cfg.Log.File.Path = old.Log.File.Path
	} else {
		cfg.Log.File.Path = old.Log.Path
	}
	cfg.Log.StdOut.Level = old.Log.StdOut.Level
	cfg.Log.StdOut.Enabled = old.Log.StdOut.Enabled
	cfg.Log.StdOut.Format = old.Log.StdOut.Format
	cfg.Log.StdOut.EnableANSI = old.Log.StdOut.EnableANSI
	cfg.Log.File.Enabled = old.Log.File.Enabled
	cfg.Log.File.Level = old.Log.File.Level
	cfg.Log.File.Format = old.Log.File.Format

	tmpFilePath := fp + ".tmp"
	f, err := os.Create(tmpFilePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	enc := yaml.NewEncoder(f)
	enc.SetIndent(2)
	if err := enc.Encode(cfg); err != nil {
		return fmt.Errorf("failed to encode config file: %w", err)
	} else if err := f.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	} else if err := f.Close(); err != nil {
		return fmt.Errorf("failed to close file: %w", err)
	} else if err := os.Rename(tmpFilePath, fp); err != nil {
		return fmt.Errorf("failed to rename file: %w", err)
	}
	return nil
}
