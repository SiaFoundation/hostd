package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	stdlog "log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/core/wallet"
	"go.sia.tech/hostd/api"
	"go.sia.tech/hostd/build"
	"go.sia.tech/hostd/config"
	"go.sia.tech/jape"
	"go.sia.tech/web/hostd"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

var (
	cfg = config.Config{
		Directory:      ".",                              // default to current directory
		RecoveryPhrase: os.Getenv(walletSeedEnvVariable), // default to env variable

		HTTP: config.HTTP{
			Address:  defaultAPIAddr,
			Password: os.Getenv(apiPasswordEnvVariable),
		},
		Consensus: config.Consensus{
			GatewayAddress: defaultGatewayAddr,
			Bootstrap:      true,
		},
		RHP2: config.RHP2{
			Address: defaultRHP2Addr,
		},
		RHP3: config.RHP3{
			TCPAddress:       defaultRHP3TCPAddr,
			WebSocketAddress: defaultRHP3WSAddr,
		},
		Log: config.Log{
			Level: "info",
			Path:  os.Getenv(logPathEnvVariable),
		},
		Prometheus: config.Prometheus{
			Address:  defaultPrometheusAddr,
			Password: os.Getenv(apiPasswordEnvVariable),
		},
	}

	disableStdin bool
)

func readPasswordInput(context string) (string, error) {
	fmt.Printf("%s: ", context)
	input, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	return string(input), err
}

// mustSetAPIPassword prompts the user to enter an API password if one is not
// already set via environment variable or config file.
func mustSetAPIPassword(log *zap.Logger) {
	if len(cfg.HTTP.Password) != 0 {
		return
	} else if disableStdin {
		log.Fatal("API password must be set via environment variable or config file when --env flag is set")
	}

	for {
		fmt.Println("Please choose a password to unlock the UI and API.")
		fmt.Println("(The password must be at least 4 characters.)")
		var err error
		cfg.HTTP.Password, err = readPasswordInput("Enter password")
		if err != nil {
			log.Fatal("Could not read password", zap.Error(err))
		}

		if len(cfg.HTTP.Password) >= 4 {
			break
		}

		fmt.Println("\033[31mPassword must be at least 4 characters!\033[0m")
		fmt.Println("")
	}
}

func mustGetSeedPhrase(log *zap.Logger) string {
	// retry until a valid seed phrase is entered
	for {
		fmt.Println("")
		fmt.Println("Type in your 12-word seed phrase and press enter. If you do not have a seed phrase yet, type 'seed' to generate one.")
		phrase, err := readPasswordInput("Enter seed phrase")
		if err != nil {
			log.Fatal("Could not read seed phrase", zap.Error(err))
		}

		if strings.ToLower(strings.TrimSpace(phrase)) == "seed" {
			// generate a new seed phrase
			var seed [32]byte
			phrase = wallet.NewSeedPhrase()
			if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
				panic(err)
			}
			key := wallet.KeyFromSeed(&seed, 0)
			fmt.Println("")
			fmt.Println("A new seed phrase has been generated below. \033[1mWrite it down and keep it safe.\033[0m")
			fmt.Println("Your seed phrase is the only way to recover your Siacoin. If you lose your seed phrase, you will also lose your Siacoin.")
			fmt.Println("You will need to re-enter this seed phrase every time you start hostd.")
			fmt.Println("")
			fmt.Println("\033[34;1mSeed Phrase:\033[0m", phrase)
			fmt.Println("\033[34;1mWallet Address:\033[0m", types.StandardUnlockHash(key.PublicKey()))

			// confirm seed phrase
			for {
				fmt.Println("")
				fmt.Println("\033[1mPlease confirm your seed phrase to continue.\033[0m")
				confirmPhrase, err := readPasswordInput("Enter seed phrase")
				if err != nil {
					log.Fatal("Could not read seed phrase", zap.Error(err))
				} else if confirmPhrase == phrase {
					return phrase
				}

				fmt.Println("\033[31mSeed phrases do not match!\033[0m")
				fmt.Println("You entered:", confirmPhrase)
				fmt.Println("Actual phrase:", phrase)
			}
		}

		var seed [32]byte
		err = wallet.SeedFromPhrase(&seed, phrase)
		if err == nil {
			// valid seed phrase
			return phrase
		}
		fmt.Println("\033[31mInvalid seed phrase:\033[0m", err)
		fmt.Println("You entered:", phrase)
	}
}

func startAPIListener(log *zap.Logger) (l net.Listener, err error) {
	addr, port, err := net.SplitHostPort(cfg.HTTP.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to parse API address: %w", err)
	}

	// if the address is not localhost, listen on the address as-is
	if addr != "localhost" {
		return net.Listen("tcp", cfg.HTTP.Address)
	}

	// localhost fails on some new installs of Windows 11, so try a few
	// different addresses
	tryAddresses := []string{
		net.JoinHostPort("localhost", port), // original address
		net.JoinHostPort("127.0.0.1", port), // IPv4 loopback
		net.JoinHostPort("::1", port),       // IPv6 loopback
	}

	for _, addr := range tryAddresses {
		l, err = net.Listen("tcp", addr)
		if err == nil {
			return
		}
		log.Warn("failed to listen on address", zap.String("address", addr), zap.Error(err))
	}
	return
}

// mustSetWalletkey prompts the user to enter a wallet seed phrase if one is not
// already set via environment variable or config file.
func mustSetWalletkey(log *zap.Logger) {
	if len(cfg.RecoveryPhrase) != 0 {
		return
	} else if disableStdin {
		log.Fatal("Wallet seed must be set via environment variable or config file when --env flag is set")
	}

	cfg.RecoveryPhrase = mustGetSeedPhrase(log)
}

// tryLoadConfig loads the config file specified by the HOSTD_CONFIG_PATH. If
// the config file does not exist, it will not be loaded.
func tryLoadConfig(log *zap.Logger) {
	configPath := "hostd.yml"
	if str := os.Getenv(configPathEnvVariable); len(str) != 0 {
		configPath = str
	}

	// If the config file doesn't exist, don't try to load it.
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		return
	}

	f, err := os.Open(configPath)
	if err != nil {
		log.Fatal("failed to open config file", zap.Error(err))
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)

	if err := dec.Decode(&cfg); err != nil {
		log.Fatal("failed to decode config file", zap.Error(err))
	}
}

func main() {
	// configure console logging note: this is configured before anything else
	// to have consistent logging. File logging will be added after the cli
	// flags and config is parsed
	consoleCfg := zap.NewProductionEncoderConfig()
	consoleCfg.TimeKey = "" // prevent duplicate timestamps
	consoleCfg.EncodeTime = zapcore.RFC3339TimeEncoder
	consoleCfg.EncodeDuration = zapcore.StringDurationEncoder
	consoleCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
	consoleCfg.StacktraceKey = ""
	consoleCfg.CallerKey = ""
	consoleEncoder := zapcore.NewConsoleEncoder(consoleCfg)

	// only log info messages to console unless stdout logging is enabled
	consoleCore := zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), zap.NewAtomicLevelAt(zap.InfoLevel))
	log := zap.New(consoleCore, zap.AddCaller())
	defer log.Sync()
	// redirect stdlib log to zap
	zap.RedirectStdLog(log.Named("stdlib"))

	// attempt to load the config file first, command line flags will override
	// any values set in the config file
	tryLoadConfig(log)

	// global
	flag.StringVar(&cfg.Name, "name", cfg.Name, "a friendly name for the host, only used for display")
	flag.StringVar(&cfg.Directory, "dir", cfg.Directory, "directory to store hostd metadata")
	flag.BoolVar(&disableStdin, "env", false, "disable stdin prompts for environment variables (default false)")
	// consensus
	flag.StringVar(&cfg.Consensus.GatewayAddress, "rpc", cfg.Consensus.GatewayAddress, "address to listen on for peer connections")
	flag.BoolVar(&cfg.Consensus.Bootstrap, "bootstrap", cfg.Consensus.Bootstrap, "bootstrap the gateway and consensus modules")
	// rhp
	flag.StringVar(&cfg.RHP2.Address, "rhp2", cfg.RHP2.Address, "address to listen on for RHP2 connections")
	flag.StringVar(&cfg.RHP3.TCPAddress, "rhp3.tcp", cfg.RHP3.TCPAddress, "address to listen on for TCP RHP3 connections")
	flag.StringVar(&cfg.RHP3.WebSocketAddress, "rhp3.ws", cfg.RHP3.WebSocketAddress, "address to listen on for WebSocket RHP3 connections")
	// http
	flag.StringVar(&cfg.HTTP.Address, "http", cfg.HTTP.Address, "address to serve API on")
	// prometheus
	flag.StringVar(&cfg.Prometheus.Address, "prometheus", "notset", "address to serve Prometheus metrics API on")
	// log
	flag.StringVar(&cfg.Log.Level, "log.level", cfg.Log.Level, "log level (debug, info, warn, error)")

	promIsSet := cfg.Prometheus.Address != "notset"

	switch flag.Arg(0) {
	case "version":
		fmt.Println("hostd", build.Version())
		fmt.Println("Network", build.NetworkName())
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
	}

	// check that the API password and wallet seed are set
	mustSetAPIPassword(log)
	mustSetWalletkey(log)

	log.Info("hostd", zap.String("version", build.Version()), zap.String("network", build.NetworkName()), zap.String("commit", build.Commit()), zap.Time("buildDate", build.Time()))

	// configure logging
	var level zap.AtomicLevel
	switch cfg.Log.Level {
	case "debug":
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		log.Fatal("invalid log level", zap.String("level", cfg.Log.Level))
	}

	// create the data directory if it does not already exist
	if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
		log.Fatal("unable to create config directory", zap.Error(err))
	}

	// set the log path to the data dir if it is not already set note: this
	// must happen after CLI flags are parsed so that the data directory can be
	// specified via the command line and environment variable
	if len(cfg.Log.Path) == 0 {
		cfg.Log.Path = cfg.Directory
	}

	// configure file logging
	fileCfg := zap.NewProductionEncoderConfig()
	fileEncoder := zapcore.NewJSONEncoder(fileCfg)

	fileWriter, closeFn, err := zap.Open(filepath.Join(cfg.Log.Path, "hostd.log"))
	if err != nil {
		fmt.Println("failed to open log file:", err)
		os.Exit(1)
	}
	defer closeFn()

	// wrap the logger to log to both stdout and the log file
	log = log.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		// use a tee to log to both stdout and the log file
		return zapcore.NewTee(
			zapcore.NewCore(fileEncoder, zapcore.Lock(fileWriter), level),
			zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), level),
		)
	}))

	var seed [32]byte
	if err := wallet.SeedFromPhrase(&seed, cfg.RecoveryPhrase); err != nil {
		log.Fatal("failed to load wallet", zap.Error(err))
	}
	walletKey := wallet.KeyFromSeed(&seed, 0)

	if err := os.MkdirAll(cfg.Directory, 0700); err != nil {
		log.Fatal("unable to create config directory", zap.Error(err))
	}

	apiListener, err := startAPIListener(log)
	if err != nil {
		log.Fatal("failed to listen on API address", zap.Error(err), zap.String("address", cfg.HTTP.Address))
	}
	defer apiListener.Close()

	rhp3WSListener, err := net.Listen("tcp", cfg.RHP3.WebSocketAddress)
	if err != nil {
		log.Fatal("failed to listen on RHP3 WebSocket address", zap.Error(err), zap.String("address", cfg.RHP3.WebSocketAddress))
	}
	defer rhp3WSListener.Close()

	node, hostKey, err := newNode(walletKey, log)
	if err != nil {
		log.Fatal("failed to create node", zap.Error(err))
	}
	defer node.Close()

	auth := jape.BasicAuth(cfg.HTTP.Password)
	web := http.Server{
		Handler: webRouter{
			api: auth(api.NewServer(cfg.Name, hostKey.PublicKey(), node.a, node.wh, node.g, node.cm, node.tp, node.contracts, node.accounts, node.storage, node.sessions, node.metrics, node.settings, node.w, log.Named("api"))),
			ui:  hostd.Handler(),
		},
		ReadTimeout: 30 * time.Second,
	}
	defer web.Close()

	rhp3WS := http.Server{
		Handler:     node.rhp3.WebSocketHandler(),
		ReadTimeout: 30 * time.Second,
		TLSConfig:   node.settings.RHP3TLSConfig(),
		ErrorLog:    stdlog.New(io.Discard, "", 0),
	}
	defer rhp3WS.Close()

	go func() {
		err := rhp3WS.ServeTLS(rhp3WSListener, "", "")
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("failed to serve rhp3 websocket", zap.Error(err))
		}
	}()

	if !promIsSet {
		log.Info("hostd started", zap.String("hostKey", hostKey.PublicKey().String()), zap.String("api", apiListener.Addr().String()), zap.String("p2p", string(node.g.Address())), zap.String("rhp2", node.rhp2.LocalAddr()), zap.String("rhp3", node.rhp3.LocalAddr()))
	}

	go func() {
		err := web.Serve(apiListener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("failed to serve web", zap.Error(err))
		}
	}()

	if promIsSet {
		prometheusListener, err := net.Listen("tcp", cfg.Prometheus.Address)
		if err != nil {
			log.Fatal("failed to listen on Prometheus metrics API address", zap.Error(err), zap.String("address", cfg.Prometheus.Address))
		}
		defer prometheusListener.Close()

		prometheusauth := jape.BasicAuth(cfg.Prometheus.Password)
		prometheusweb := http.Server{
			Handler: prometheusRouter{
				api: prometheusauth(api.NewServerPrometheus(cfg.Name, hostKey.PublicKey(), node.a, node.g, node.cm, node.tp, node.contracts, node.accounts, node.storage, node.sessions, node.metrics, node.settings, node.w, log.Named("prometheus"))),
			},
			ReadTimeout: 30 * time.Second,
		}
		defer prometheusweb.Close()

		go func() {
			err := prometheusweb.Serve(prometheusListener)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("failed to serve prometheus", zap.Error(err))
			}
		}()

		log.Info("hostd started", zap.String("hostKey", hostKey.PublicKey().String()), zap.String("api", apiListener.Addr().String()), zap.String("p2p", string(node.g.Address())), zap.String("rhp2", node.rhp2.LocalAddr()), zap.String("rhp3", node.rhp3.LocalAddr()), zap.String("prometheus", prometheusListener.Addr().String()))
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	log.Info("shutting down...")
	time.AfterFunc(5*time.Minute, func() {
		log.Fatal("failed to shut down within 5 minutes")
	})
}
