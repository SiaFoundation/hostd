package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/hostd/build"
	"go.sia.tech/hostd/config"
	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

const (
	walletSeedEnvVar  = "HOSTD_WALLET_SEED"
	apiPasswordEnvVar = "HOSTD_API_PASSWORD"
	configFileEnvVar  = "HOSTD_CONFIG_FILE"
	logFileEnvVar     = "HOSTD_LOG_FILE_PATH"
)

var (
	cfg = config.Config{
		Directory:      ".",                         // default to current directory
		RecoveryPhrase: os.Getenv(walletSeedEnvVar), // default to env variable
		AutoOpenWebUI:  true,

		HTTP: config.HTTP{
			Address:  "127.0.0.1:9980",
			Password: os.Getenv(apiPasswordEnvVar),
		},
		Explorer: config.ExplorerData{
			URL: "https://api.siascan.com",
		},
		Syncer: config.Syncer{
			Address:   ":9981",
			Bootstrap: true,
		},
		Consensus: config.Consensus{
			Network:        "mainnet",
			IndexBatchSize: 1000,
		},
		RHP2: config.RHP2{
			Address: ":9982",
		},
		RHP3: config.RHP3{
			TCPAddress: ":9983",
		},
		Log: config.Log{
			Path:  os.Getenv(logFileEnvVar), // deprecated. included for compatibility.
			Level: "info",
			File: config.LogFile{
				Enabled: true,
				Format:  "json",
				Path:    os.Getenv(logFileEnvVar),
			},
			StdOut: config.StdOut{
				Enabled:    true,
				Format:     "human",
				EnableANSI: runtime.GOOS != "windows",
			},
		},
	}

	disableStdin bool
)

func openBrowser(url string) error {
	switch runtime.GOOS {
	case "linux":
		return exec.Command("xdg-open", url).Start()
	case "windows":
		return exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		return exec.Command("open", url).Start()
	default:
		return fmt.Errorf("unsupported platform %q", runtime.GOOS)
	}
}

// tryLoadConfig loads the config file specified by the HOSTD_CONFIG_FILE. If
// the config file does not exist, it will not be loaded.
func tryLoadConfig() {
	configPath := "hostd.yml"
	if str := os.Getenv(configFileEnvVar); str != "" {
		configPath = str
	}

	// If the config file doesn't exist, don't try to load it.
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return
	}

	f, err := os.Open(configPath)
	if err != nil {
		stdoutFatalError("failed to open config file: " + err.Error())
		return
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)

	if err := dec.Decode(&cfg); err != nil {
		fmt.Println("failed to decode config file:", err)
		os.Exit(1)
	}
}

// jsonEncoder returns a zapcore.Encoder that encodes logs as JSON intended for
// parsing.
func jsonEncoder() zapcore.Encoder {
	cfg := zap.NewProductionEncoderConfig()
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	return zapcore.NewJSONEncoder(cfg)
}

// humanEncoder returns a zapcore.Encoder that encodes logs as human-readable
// text.
func humanEncoder(showColors bool) zapcore.Encoder {
	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "" // prevent duplicate timestamps
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	cfg.EncodeDuration = zapcore.StringDurationEncoder

	if showColors {
		cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	} else {
		cfg.EncodeLevel = zapcore.CapitalLevelEncoder
	}

	cfg.StacktraceKey = ""
	cfg.CallerKey = ""
	return zapcore.NewConsoleEncoder(cfg)
}

func parseLogLevel(level string) zap.AtomicLevel {
	switch level {
	case "debug":
		return zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		return zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		return zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		return zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		fmt.Printf("invalid log level %q", level)
		os.Exit(1)
	}
	panic("unreachable")
}

func main() {
	// attempt to load the config file first, command line flags will override
	// any values set in the config file
	tryLoadConfig()

	// global
	flag.StringVar(&cfg.Name, "name", cfg.Name, "a friendly name for the host, only used for display")
	flag.StringVar(&cfg.Directory, "dir", cfg.Directory, "directory to store hostd metadata")
	flag.BoolVar(&disableStdin, "env", false, "disable stdin prompts for environment variables (default false)")
	flag.BoolVar(&cfg.AutoOpenWebUI, "openui", cfg.AutoOpenWebUI, "automatically open the web UI on startup")
	// syncer
	flag.StringVar(&cfg.Syncer.Address, "syncer.address", cfg.Syncer.Address, "address to listen on for peer connections")
	flag.BoolVar(&cfg.Syncer.Bootstrap, "syncer.bootstrap", cfg.Syncer.Bootstrap, "bootstrap the gateway and consensus modules")
	// consensus
	flag.StringVar(&cfg.Consensus.Network, "network", cfg.Consensus.Network, "network name (mainnet, testnet, etc)")
	// rhp
	flag.StringVar(&cfg.RHP2.Address, "rhp2", cfg.RHP2.Address, "address to listen on for RHP2 connections")
	flag.StringVar(&cfg.RHP3.TCPAddress, "rhp3", cfg.RHP3.TCPAddress, "address to listen on for TCP RHP3 connections")
	// http
	flag.StringVar(&cfg.HTTP.Address, "http", cfg.HTTP.Address, "address to serve API on")
	// log
	flag.StringVar(&cfg.Log.Level, "log.level", cfg.Log.Level, "log level (debug, info, warn, error)")
	flag.Parse()

	switch flag.Arg(0) {
	case "version":
		fmt.Println("hostd", build.Version())
		fmt.Println("Commit:", build.Commit())
		fmt.Println("Build Date:", build.Time())
		return
	case "seed":
		var seed [32]byte
		phrase := wallet.NewSeedPhrase()
		if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		key := wallet.KeyFromSeed(&seed, 0)
		fmt.Println("Recovery Phrase:", phrase)
		fmt.Println("Address", types.StandardUnlockHash(key.PublicKey()))
		return
	case "config":
		buildConfig()
		return
	case "rebuild":
		fp := flag.Arg(1)

		levelStr := cfg.Log.StdOut.Level
		if levelStr == "" {
			levelStr = cfg.Log.Level
		}

		level := parseLogLevel(levelStr)
		core := zapcore.NewCore(humanEncoder(cfg.Log.StdOut.EnableANSI), zapcore.Lock(os.Stdout), level)
		log := zap.New(core, zap.AddCaller())
		defer log.Sync()

		db, err := sqlite.OpenDatabase(fp, log)
		if err != nil {
			log.Fatal("failed to open database", zap.Error(err))
		}
		defer db.Close()

		if err := db.CheckContractAccountFunding(); err != nil {
			log.Fatal("failed to check contract account funding", zap.Error(err))
		} else if err := db.RecalcContractAccountFunding(); err != nil {
			log.Fatal("failed to recalculate contract account funding", zap.Error(err))
		} else if err := db.CheckContractAccountFunding(); err != nil {
			log.Fatal("failed to check contract account funding", zap.Error(err))
		} else if err := db.Vacuum(); err != nil {
			log.Fatal("failed to vacuum database", zap.Error(err))
		}
		return
	case "":
		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		// check that the API password is set
		if cfg.HTTP.Password == "" {
			if disableStdin {
				stdoutFatalError("API password must be set via environment variable or config file when --env flag is set")
				return
			}
			setAPIPassword()
		}

		// check that the wallet seed is set
		if cfg.RecoveryPhrase == "" {
			if disableStdin {
				stdoutFatalError("Wallet seed must be set via environment variable or config file when --env flag is set")
				return
			}
			setSeedPhrase()
		}

		// create the data directory if it does not already exist
		if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
			stdoutFatalError("unable to create config directory: " + err.Error())
		}

		// configure the logger
		if !cfg.Log.StdOut.Enabled && !cfg.Log.File.Enabled {
			stdoutFatalError("At least one of stdout or file logging must be enabled")
			return
		}

		// normalize log level
		if cfg.Log.Level == "" {
			cfg.Log.Level = "info"
		}

		var logCores []zapcore.Core
		if cfg.Log.StdOut.Enabled {
			// if no log level is set for stdout, use the global log level
			if cfg.Log.StdOut.Level == "" {
				cfg.Log.StdOut.Level = cfg.Log.Level
			}

			var encoder zapcore.Encoder
			switch cfg.Log.StdOut.Format {
			case "json":
				encoder = jsonEncoder()
			default: // stdout defaults to human
				encoder = humanEncoder(cfg.Log.StdOut.EnableANSI)
			}

			// create the stdout logger
			level := parseLogLevel(cfg.Log.StdOut.Level)
			logCores = append(logCores, zapcore.NewCore(encoder, zapcore.Lock(os.Stdout), level))
		}

		if cfg.Log.File.Enabled {
			// if no log level is set for file, use the global log level
			if cfg.Log.File.Level == "" {
				cfg.Log.File.Level = cfg.Log.Level
			}

			// normalize log path
			if cfg.Log.File.Path == "" {
				// If the log path is not set, try the deprecated log path. If that
				// is also not set, default to hostd.log in the data directory.
				if cfg.Log.Path != "" {
					cfg.Log.File.Path = filepath.Join(cfg.Log.Path, "hostd.log")
				} else {
					cfg.Log.File.Path = filepath.Join(cfg.Directory, "hostd.log")
				}
			}

			// configure file logging
			var encoder zapcore.Encoder
			switch cfg.Log.File.Format {
			case "human":
				encoder = humanEncoder(false) // disable colors in file log
			default: // log file defaults to JSON
				encoder = jsonEncoder()
			}

			fileWriter, closeFn, err := zap.Open(cfg.Log.File.Path)
			if err != nil {
				stdoutFatalError("failed to open log file: " + err.Error())
				return
			}
			defer closeFn()

			// create the file logger
			level := parseLogLevel(cfg.Log.File.Level)
			logCores = append(logCores, zapcore.NewCore(encoder, zapcore.Lock(fileWriter), level))
		}

		var log *zap.Logger
		if len(logCores) == 1 {
			log = zap.New(logCores[0], zap.AddCaller())
		} else {
			log = zap.New(zapcore.NewTee(logCores...), zap.AddCaller())
		}
		defer log.Sync()

		// redirect stdlib log to zap
		zap.RedirectStdLog(log.Named("stdlib"))

		log.Info("hostd", zap.String("version", build.Version()), zap.String("network", cfg.Consensus.Network), zap.String("commit", build.Commit()), zap.Time("buildDate", build.Time()))

		var seed [32]byte
		if err := wallet.SeedFromPhrase(&seed, cfg.RecoveryPhrase); err != nil {
			log.Fatal("failed to load wallet", zap.Error(err))
		}
		walletKey := wallet.KeyFromSeed(&seed, 0)

		if err := runNode(ctx, cfg, walletKey, log); err != nil {
			log.Error("failed to start node", zap.Error(err))
		}
	default:
		stdoutFatalError("unknown command: " + flag.Arg(0))
	}
}
