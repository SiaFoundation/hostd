package config

import (
	"bytes"
	"encoding/hex"
	"os"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
	"lukechampine.com/frand"
)

func TestUpgradeV235(t *testing.T) {
	old := configV235{
		Name:           hex.EncodeToString(frand.Bytes(10)),
		Directory:      hex.EncodeToString(frand.Bytes(10)),
		RecoveryPhrase: hex.EncodeToString(frand.Bytes(10)),
		AutoOpenWebUI:  true,
		HTTP: struct {
			Address  string `yaml:"address,omitempty"`
			Password string `yaml:"password,omitempty"`
		}{
			Address:  hex.EncodeToString(frand.Bytes(10)),
			Password: hex.EncodeToString(frand.Bytes(10)),
		},
		Syncer: struct {
			Address    string   `yaml:"address,omitempty"`
			Bootstrap  bool     `yaml:"bootstrap,omitempty"`
			EnableUPnP bool     `yaml:"enableUPnP,omitempty"`
			Peers      []string `yaml:"peers,omitempty"`
		}{
			Address:    hex.EncodeToString(frand.Bytes(10)),
			Bootstrap:  true,
			EnableUPnP: true,
			Peers:      []string{hex.EncodeToString(frand.Bytes(10))},
		},
		Consensus: struct {
			Network        string `yaml:"network,omitempty"`
			IndexBatchSize int    `yaml:"indexBatchSize,omitempty"`
		}{
			Network:        hex.EncodeToString(frand.Bytes(10)),
			IndexBatchSize: frand.Intn(1000),
		},
		Explorer: struct {
			Disable bool   `yaml:"disable,omitempty"`
			URL     string `yaml:"url,omitempty"`
		}{
			Disable: false,
			URL:     hex.EncodeToString(frand.Bytes(10)),
		},
		RHP2: struct {
			Address string `yaml:"address,omitempty"`
		}{
			Address: hex.EncodeToString(frand.Bytes(10)),
		},
		RHP3: struct {
			TCPAddress string `yaml:"tcp,omitempty"`
		}{
			TCPAddress: hex.EncodeToString(frand.Bytes(10)),
		},
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
			QUIC: struct {
				CertPath string `yaml:"certPath,omitempty"`
				KeyPath  string `yaml:"keyPath,omitempty"`
			}{
				CertPath: hex.EncodeToString(frand.Bytes(10)),
				KeyPath:  hex.EncodeToString(frand.Bytes(10)),
			},
			ListenAddresses: []struct {
				Protocol string `yaml:"protocol,omitempty"`
				Address  string `yaml:"address,omitempty"`
			}{
				{
					Protocol: "siamux",
					Address:  hex.EncodeToString(frand.Bytes(10)),
				},
				{
					Protocol: "quic",
					Address:  hex.EncodeToString(frand.Bytes(10)),
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
				// Path is the path of the log file.
				Path string `yaml:"path,omitempty"`
			} `yaml:"file,omitempty"`
		}{
			Level: "debug",
			StdOut: struct {
				Level      string `yaml:"level,omitempty"` // override the stdout log level
				Enabled    bool   `yaml:"enabled,omitempty"`
				Format     string `yaml:"format,omitempty"`
				EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
			}{
				Level:      "debug",
				Enabled:    true,
				Format:     hex.EncodeToString(frand.Bytes(10)),
				EnableANSI: true,
			},
			File: struct {
				Enabled bool   `yaml:"enabled,omitempty"`
				Level   string `yaml:"level,omitempty"` // override the file log level
				Format  string `yaml:"format,omitempty"`
				// Path is the path of the log file.
				Path string `yaml:"path,omitempty"`
			}{
				Enabled: true,
				Level:   "debug",
				Format:  hex.EncodeToString(frand.Bytes(10)),
				Path:    hex.EncodeToString(frand.Bytes(10)),
			},
		},
	}

	fp := t.TempDir() + "/hostd.yml"
	f, err := os.Create(fp)
	if err != nil {
		t.Fatalf("failed to create config file: %v", err)
	}
	defer f.Close()

	enc := yaml.NewEncoder(f)
	if err := enc.Encode(&old); err != nil {
		t.Fatalf("failed to encode config file: %v", err)
	} else if err := f.Sync(); err != nil {
		t.Fatalf("failed to sync config file: %v", err)
	} else if err := f.Close(); err != nil {
		t.Fatalf("failed to close config file: %v", err)
	}

	buf, err := os.ReadFile(fp)
	if err != nil {
		t.Fatalf("failed to read config file: %v", err)
	}
	r := bytes.NewReader(buf)

	var upgraded Config
	if err := updateConfigV235(fp, r, &upgraded); err != nil {
		t.Fatalf("failed to upgrade config: %v", err)
	}

	// read the config from disk to ensure it is rewritten correctly
	var cfg Config
	if err := LoadFile(fp, &cfg); err != nil {
		t.Fatalf("failed to load config file: %v", err)
	} else if !reflect.DeepEqual(cfg, upgraded) {
		t.Fatalf("config on disk does not match expected config:\nexpected: %+v\ngot: %+v", upgraded, cfg)
	}
}

func TestUpgradeV112(t *testing.T) {
	old := configV112{
		Name:           hex.EncodeToString(frand.Bytes(10)),
		Directory:      hex.EncodeToString(frand.Bytes(10)),
		RecoveryPhrase: hex.EncodeToString(frand.Bytes(10)),
		AutoOpenWebUI:  true,
		HTTP: struct {
			Address  string `yaml:"address,omitempty"`
			Password string `yaml:"password,omitempty"`
		}{
			Address:  hex.EncodeToString(frand.Bytes(10)),
			Password: hex.EncodeToString(frand.Bytes(10)),
		},
		Consensus: struct {
			GatewayAddress string   `yaml:"gatewayAddress,omitempty"`
			Bootstrap      bool     `yaml:"bootstrap,omitempty"`
			Peers          []string `yaml:"peers,omitempty"`
		}{
			GatewayAddress: hex.EncodeToString(frand.Bytes(10)),
			Bootstrap:      true,
			Peers:          []string{hex.EncodeToString(frand.Bytes(10))},
		},
		Explorer: struct {
			Disable bool   `yaml:"disable,omitempty"`
			URL     string `yaml:"url,omitempty"`
		}{
			Disable: false,
			URL:     hex.EncodeToString(frand.Bytes(10)),
		},
		RHP2: struct {
			Address string `yaml:"address,omitempty"`
		}{
			Address: hex.EncodeToString(frand.Bytes(10)),
		},
		RHP3: struct {
			TCPAddress       string `yaml:"tcp,omitempty"`
			WebSocketAddress string `yaml:"websocket,omitempty"`
			CertPath         string `yaml:"certPath,omitempty"`
			KeyPath          string `yaml:"keyPath,omitempty"`
		}{
			TCPAddress:       hex.EncodeToString(frand.Bytes(10)),
			WebSocketAddress: hex.EncodeToString(frand.Bytes(10)),
			CertPath:         hex.EncodeToString(frand.Bytes(10)),
			KeyPath:          hex.EncodeToString(frand.Bytes(10)),
		},
		Log: struct {
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
		}{
			Path:  hex.EncodeToString(frand.Bytes(10)),
			Level: "debug",
			StdOut: struct {
				Level      string `yaml:"level,omitempty"` // override the stdout log level
				Enabled    bool   `yaml:"enabled,omitempty"`
				Format     string `yaml:"format,omitempty"`
				EnableANSI bool   `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
			}{
				Level:      "debug",
				Enabled:    true,
				Format:     hex.EncodeToString(frand.Bytes(10)),
				EnableANSI: true,
			},
			File: struct {
				Enabled bool   `yaml:"enabled,omitempty"`
				Level   string `yaml:"level,omitempty"` // override the file log level
				Format  string `yaml:"format,omitempty"`
				// Path is the path of the log file.
				Path string `yaml:"path,omitempty"`
			}{
				Enabled: true,
				Level:   "debug",
				Format:  hex.EncodeToString(frand.Bytes(10)),
				Path:    hex.EncodeToString(frand.Bytes(10)),
			},
		},
	}

	fp := t.TempDir() + "/hostd.yml"
	f, err := os.Create(fp)
	if err != nil {
		t.Fatalf("failed to create config file: %v", err)
	}
	defer f.Close()

	enc := yaml.NewEncoder(f)
	if err := enc.Encode(&old); err != nil {
		t.Fatalf("failed to encode config file: %v", err)
	} else if err := f.Sync(); err != nil {
		t.Fatalf("failed to sync config file: %v", err)
	} else if err := f.Close(); err != nil {
		t.Fatalf("failed to close config file: %v", err)
	}

	buf, err := os.ReadFile(fp)
	if err != nil {
		t.Fatalf("failed to read config file: %v", err)
	}
	r := bytes.NewReader(buf)

	var upgraded Config
	if err := updateConfigV112(fp, r, &upgraded); err != nil {
		t.Fatalf("failed to upgrade config: %v", err)
	}

	// read the config from disk to ensure it is rewritten correctly
	var cfg Config
	if err := LoadFile(fp, &cfg); err != nil {
		t.Fatalf("failed to load config file: %v", err)
	} else if !reflect.DeepEqual(cfg, upgraded) {
		t.Fatalf("config on disk does not match expected config:\nexpected: %+v\ngot: %+v", upgraded, cfg)
	}
}
