package main

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	"lukechampine.com/flagg"
)

const (
	walletSeedEnvVar  = "HOSTD_WALLET_SEED"
	apiPasswordEnvVar = "HOSTD_API_PASSWORD"
	configFileEnvVar  = "HOSTD_CONFIG_FILE"
	logFileEnvVar     = "HOSTD_LOG_FILE_PATH"
)

var (
	cfg = config.Config{
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
		RHP4: config.RHP4{
			ListenAddresses: []config.RHP4ListenAddress{
				{
					Protocol: "tcp",
					Address:  ":9984",
				},
			},
		},
		Log: config.Log{
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

func tryConfigPaths() []string {
	if str := os.Getenv(configFileEnvVar); str != "" {
		return []string{str}
	}

	paths := []string{
		"hostd.yml",
	}
	switch runtime.GOOS {
	case "windows":
		paths = append(paths, filepath.Join(os.Getenv("APPDATA"), "hostd", "hostd.yml"))
	case "darwin":
		paths = append(paths, filepath.Join(os.Getenv("HOME"), "Library", "Application Support", "hostd", "hostd.yml"))
	case "linux", "freebsd", "openbsd":
		paths = append(paths,
			filepath.Join("/etc", "hostd", "hostd.yml"), // prefer /etc over /var
			filepath.Join("/var", "lib", "hostd", "hostd.yml"),
			filepath.Join(os.Getenv("HOME"), ".hostd", "hostd.yml"),
		)
	}
	return paths
}

// tryLoadConfig tries to load the config file. It will try multiple locations
// based on GOOS starting with PWD/hostd.yml. If the file does not exist, it will
// try the next location. If an error occurs while loading the file, it will
// print the error and exit. If the config is successfully loaded, the path to
// the config file is returned.
func tryLoadConfig() string {
	for _, fp := range tryConfigPaths() {
		if err := config.LoadFile(fp, &cfg); err == nil {
			return fp
		} else if !errors.Is(err, os.ErrNotExist) {
			checkFatalError("failed to load config file", err)
		}
	}
	return ""
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

const (
	rootUsage = `Usage:
hostd [flags] [command]

Run 'hostd' with no commands to start the storage provider daemon.

Commands:
	version		Print the hostd version
	seed		Generate a new wallet seed and print the corresponding address
	config		Print the default hostd config
	recalculate	Recalculate the contract account funding in the SQLite3 database
	sqlite3		Perform various operations on the SQLite3 database
`

	versionUsage = `Usage:
hostd version

Print the version of hostd.`

	seedUsage = `Usage:
hostd seed

Generate a secure BIP-39 seed phrase and corresponding address. The seed phrase should be added to the config file or set as the HOSTD_WALLET_SEED environment variable.
`

	configUsage = `Usage:
hostd config

Interactively configure hostd. The resulting config will be saved to hostd.yml or the file specified by the HOSTD_CONFIG_FILE environment variable.
`

	recalculateUsage = `Usage:
hostd recalculate <srcPath>

Recalculates the contract account funding in the SQLite3 database. This command is not safe to run while the host is running.
`

	sqlite3Usage = `Usage:
hostd sqlite3 [subcommand]

Perform various operations on the SQLite3 database.

Commands:
	backup	Create a backup of the SQLite3 database
`

	sqlite3BackupUsage = `Usage:
hostd sqlite3 backup <srcPath> <destPath>

Create a backup of the SQLite3 database at the specified path. This is safe to run while the host is running.
`
)

// checkFatalError prints an error message to stderr and exits with a 1 exit code. If err is nil, this is a no-op.
func checkFatalError(context string, err error) {
	if err == nil {
		return
	}
	os.Stderr.WriteString(fmt.Sprintf("%s: %s\n", context, err))
	os.Exit(1)
}

func initStdoutLog(colored bool, levelStr string) *zap.Logger {
	level := parseLogLevel(levelStr)
	core := zapcore.NewCore(humanEncoder(colored), zapcore.Lock(os.Stdout), level)
	return zap.New(core, zap.AddCaller())
}

func runBackupCommand(srcPath, destPath string) error {
	if err := sqlite.Backup(context.Background(), srcPath, destPath); err != nil {
		return fmt.Errorf("failed to backup database: %w", err)
	}
	return nil
}

func runRecalcCommand(srcPath string, log *zap.Logger) error {
	db, err := sqlite.OpenDatabase(srcPath, log)
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
	return nil
}

func main() {
	// attempt to load the config file, command line flags will override any
	// values set in the config file
	configPath := tryLoadConfig()
	// set the data directory to the default if it is not set
	cfg.Directory = defaultDatabasePath(cfg.Directory)
	log.Println(cfg.Directory)

	rootCmd := flagg.Root
	rootCmd.Usage = flagg.SimpleUsage(rootCmd, rootUsage)
	rootCmd.StringVar(&cfg.Name, "name", cfg.Name, "a friendly name for the host, only used for display")
	rootCmd.StringVar(&cfg.Directory, "dir", cfg.Directory, "directory to store hostd metadata")
	rootCmd.BoolVar(&disableStdin, "env", false, "disable stdin prompts for environment variables (default false)")
	rootCmd.BoolVar(&cfg.AutoOpenWebUI, "openui", cfg.AutoOpenWebUI, "automatically open the web UI on startup")
	// syncer
	rootCmd.StringVar(&cfg.Syncer.Address, "syncer.address", cfg.Syncer.Address, "address to listen on for peer connections")
	rootCmd.BoolVar(&cfg.Syncer.Bootstrap, "syncer.bootstrap", cfg.Syncer.Bootstrap, "bootstrap the gateway and consensus modules")
	// consensus
	rootCmd.StringVar(&cfg.Consensus.Network, "network", cfg.Consensus.Network, "network name (mainnet, zen, etc)")
	// rhp
	rootCmd.StringVar(&cfg.RHP2.Address, "rhp2", cfg.RHP2.Address, "address to listen on for RHP2 connections")
	rootCmd.StringVar(&cfg.RHP3.TCPAddress, "rhp3", cfg.RHP3.TCPAddress, "address to listen on for TCP RHP3 connections")
	// http
	rootCmd.StringVar(&cfg.HTTP.Address, "http", cfg.HTTP.Address, "address to serve API on")
	// log
	rootCmd.StringVar(&cfg.Log.Level, "log.level", cfg.Log.Level, "log level (debug, info, warn, error)")

	versionCmd := flagg.New("version", versionUsage)
	seedCmd := flagg.New("seed", seedUsage)
	configCmd := flagg.New("config", configUsage)
	recalculateCmd := flagg.New("recalculate", recalculateUsage)
	sqlite3Cmd := flagg.New("sqlite3", sqlite3Usage)
	sqlite3BackupCmd := flagg.New("backup", sqlite3BackupUsage)

	cmd := flagg.Parse(flagg.Tree{
		Cmd: rootCmd,
		Sub: []flagg.Tree{
			{Cmd: versionCmd},
			{Cmd: seedCmd},
			{Cmd: configCmd},
			{Cmd: recalculateCmd},
			{
				Cmd: sqlite3Cmd,
				Sub: []flagg.Tree{
					{Cmd: sqlite3BackupCmd},
				},
			},
		},
	})

	switch cmd {
	case versionCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			return
		}

		fmt.Println("hostd", build.Version())
		fmt.Println("Commit:", build.Commit())
		fmt.Println("Build Date:", build.Time())

	case seedCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			return
		}

		var seed [32]byte
		phrase := wallet.NewSeedPhrase()
		checkFatalError("failed to generate seed", wallet.SeedFromPhrase(&seed, phrase))
		key := wallet.KeyFromSeed(&seed, 0)
		fmt.Println("Recovery Phrase:", phrase)
		fmt.Println("Address", types.StandardUnlockHash(key.PublicKey()))
	case configCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			return
		}

		runConfigCmd(configPath)
	case recalculateCmd:
		if len(cmd.Args()) != 1 {
			cmd.Usage()
			return
		}

		log := initStdoutLog(cfg.Log.StdOut.EnableANSI, cfg.Log.Level)
		defer log.Sync()

		checkFatalError("command failed", runRecalcCommand(cmd.Arg(0), log))
	case sqlite3Cmd:
		cmd.Usage()
	case sqlite3BackupCmd:
		if len(cmd.Args()) != 2 {
			cmd.Usage()
			return
		}

		log := initStdoutLog(cfg.Log.StdOut.EnableANSI, cfg.Log.Level)
		defer log.Sync()

		checkFatalError("command failed", runBackupCommand(cmd.Arg(0), cmd.Arg(1)))
	case rootCmd:
		if len(cmd.Args()) != 0 {
			cmd.Usage()
			return
		}

		ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer cancel()

		// check that the API password is set
		if cfg.HTTP.Password == "" {
			if disableStdin {
				checkFatalError("API password not set", errors.New("API password must be set via environment variable or config file when --env flag is set"))
			}
			setAPIPassword()
		}

		// check that the wallet seed is set
		if cfg.RecoveryPhrase == "" {
			if disableStdin {
				checkFatalError("wallet seed not set", errors.New("wallet seed must be set via environment variable or config file when --env flag is set"))
			}
			setSeedPhrase()
		}

		// create the data directory if it does not already exist
		if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
			checkFatalError("failed to create config directory", err)
		}

		// configure the logger
		if !cfg.Log.StdOut.Enabled && !cfg.Log.File.Enabled {
			checkFatalError("logging disabled", errors.New("either stdout or file logging must be enabled"))
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
				cfg.Log.File.Path = filepath.Join(cfg.Directory, "hostd.log")
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
			checkFatalError("failed to open log file", err)
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
		checkFatalError("failed to load wallet seed", wallet.SeedFromPhrase(&seed, cfg.RecoveryPhrase))
		walletKey := wallet.KeyFromSeed(&seed, 0)

		checkFatalError("daemon startup failed", runRootCmd(ctx, cfg, walletKey, log))
	}
}
