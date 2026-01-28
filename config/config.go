package config

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

// RHP4Proto defines protocols valid for RHP4.
const (
	RHP4ProtoQUIC  RHP4Proto = "quic"
	RHP4ProtoQUIC4 RHP4Proto = "quic4"
	RHP4ProtoQUIC6 RHP4Proto = "quic6"

	RHP4ProtoTCP  RHP4Proto = "tcp"
	RHP4ProtoTCP4 RHP4Proto = "tcp4"
	RHP4ProtoTCP6 RHP4Proto = "tcp6"
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

	// RHP4Proto is the protocol used for RHP4.
	RHP4Proto string

	// RHP4ListenAddress contains the configuration for an RHP4 listen address.
	RHP4ListenAddress struct {
		Protocol RHP4Proto `yaml:"protocol,omitempty"`
		Address  string    `yaml:"address,omitempty"`
	}

	// RHP4QUIC contains the configuration for the RHP4 QUIC server.
	RHP4QUIC struct {
		CertPath string `yaml:"certPath,omitempty"`
		KeyPath  string `yaml:"keyPath,omitempty"`
	}

	// RHP4 contains the configuration for the RHP4 server.
	RHP4 struct {
		QUIC            RHP4QUIC            `yaml:"quic,omitempty"`
		ListenAddresses []RHP4ListenAddress `yaml:"listenAddresses,omitempty"`
	}

	// LogFile configures the file output of the logger.
	LogFile struct {
		Enabled bool            `yaml:"enabled,omitempty"`
		Level   zap.AtomicLevel `yaml:"level,omitempty"` // override the file log level
		Format  string          `yaml:"format,omitempty"`
		// Path is the path of the log file.
		Path string `yaml:"path,omitempty"`
	}

	// StdOut configures the standard output of the logger.
	StdOut struct {
		Enabled    bool            `yaml:"enabled,omitempty"`
		Level      zap.AtomicLevel `yaml:"level,omitempty"` // override the stdout log level
		Format     string          `yaml:"format,omitempty"`
		EnableANSI bool            `yaml:"enableANSI,omitempty"` //nolint:tagliatelle
	}

	// Storage contains the configuration for the storage subsystem.
	Storage struct {
		// EnableMerkleCache indicates whether to enable the merkle root cache.
		// This will cache the Merkle subtree roots for all sectors in the
		// database to reduce disk IO for partial sector reads.
		//
		// Enabling the cache increases the SQLite database size by
		// 8GiB per TiB of stored data (~0.8% overhead), but will
		// reduce disk IO by 1000x for random partial sector reads.
		//
		// Disable only if SSD storage space is extremely constrained.
		EnableMerkleCache bool `yaml:"enableMerkleCache,omitempty"`
	}

	// Log contains the configuration for the logger.
	Log struct {
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
		Storage   Storage      `yaml:"storage,omitempty"`
		RHP4      RHP4         `yaml:"rhp4,omitempty"`
		Log       Log          `yaml:"log,omitempty"`
	}
)

func upgrade(fp string, r io.ReadSeeker, cfg *Config) error {
	versions := []func(string, io.Reader, *Config) error{
		updateConfigV235,
		updateConfigV112,
	}
	for _, fn := range versions {
		if _, err := r.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("failed to reset reader: %w", err)
		} else if upgradeErr := fn(fp, r, cfg); upgradeErr == nil {
			break // no error means the upgrade was successful
		}
	}

	// write the updated config back to the file
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

// LoadFile loads the configuration from the provided file path.
// If the file does not exist, an error is returned.
// If the file exists but cannot be decoded, the function will attempt
// to upgrade the config file.
func LoadFile(fp string, cfg *Config) error {
	buf, err := os.ReadFile(fp)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	r := bytes.NewReader(buf)
	dec := yaml.NewDecoder(r)
	dec.KnownFields(true)

	if err := dec.Decode(cfg); err != nil {
		if upgradeErr := upgrade(fp, r, cfg); upgradeErr == nil {
			return nil
		}
		return fmt.Errorf("failed to decode config file: %w", err)
	}
	return nil
}
