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
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
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
	"gopkg.in/yaml.v3"
)

var (
	cfg = config.Config{
		Directory:      ".",                              // default to current directory
		RecoveryPhrase: os.Getenv(walletSeedEnvVariable), // default to env variable
		AutoOpenWebUI:  true,

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
			Path:  os.Getenv(logPathEnvVariable), // deprecated. included for compatibility.
			Level: "info",
			File: config.LogFile{
				Enabled: true,
				Format:  "json",
				Path:    os.Getenv(logFileEnvVariable),
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
		log.Debug("failed to listen on fallback address", zap.String("address", addr), zap.Error(err))
	}
	return
}

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

// tryLoadConfig loads the config file specified by the HOSTD_CONFIG_PATH. If
// the config file does not exist, it will not be loaded.
func tryLoadConfig() {
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
	return zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig())
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
	// consensus
	flag.StringVar(&cfg.Consensus.GatewayAddress, "rpc", cfg.Consensus.GatewayAddress, "address to listen on for peer connections")
	flag.BoolVar(&cfg.Consensus.Bootstrap, "bootstrap", cfg.Consensus.Bootstrap, "bootstrap the gateway and consensus modules")
	// rhp
	flag.StringVar(&cfg.RHP2.Address, "rhp2", cfg.RHP2.Address, "address to listen on for RHP2 connections")
	flag.StringVar(&cfg.RHP3.TCPAddress, "rhp3.tcp", cfg.RHP3.TCPAddress, "address to listen on for TCP RHP3 connections")
	flag.StringVar(&cfg.RHP3.WebSocketAddress, "rhp3.ws", cfg.RHP3.WebSocketAddress, "address to listen on for WebSocket RHP3 connections")
	// http
	flag.StringVar(&cfg.HTTP.Address, "http", cfg.HTTP.Address, "address to serve API on")
	// log
	flag.StringVar(&cfg.Log.Level, "log.level", cfg.Log.Level, "log level (debug, info, warn, error)")
	flag.Parse()

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
	case "config":
		buildConfig()
		return
	}

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

	log.Info("hostd", zap.String("version", build.Version()), zap.String("network", build.NetworkName()), zap.String("commit", build.Commit()), zap.Time("buildDate", build.Time()))

	var seed [32]byte
	if err := wallet.SeedFromPhrase(&seed, cfg.RecoveryPhrase); err != nil {
		log.Fatal("failed to load wallet", zap.Error(err))
	}
	walletKey := wallet.KeyFromSeed(&seed, 0)

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

	log.Info("hostd started", zap.String("hostKey", hostKey.PublicKey().String()), zap.String("api", apiListener.Addr().String()), zap.String("p2p", string(node.g.Address())), zap.String("rhp2", node.rhp2.LocalAddr()), zap.String("rhp3", node.rhp3.LocalAddr()))

	go func() {
		err := web.Serve(apiListener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("failed to serve web", zap.Error(err))
		}
	}()

	if cfg.AutoOpenWebUI {
		time.Sleep(time.Millisecond) // give the web server a chance to start
		_, port, err := net.SplitHostPort(apiListener.Addr().String())
		if err != nil {
			log.Debug("failed to parse API address", zap.Error(err))
		} else if err := openBrowser(fmt.Sprintf("http://127.0.0.1:%s", port)); err != nil {
			log.Debug("failed to open browser", zap.Error(err))
		}
	}

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	log.Info("shutting down...")
	time.AfterFunc(5*time.Minute, func() {
		log.Fatal("failed to shut down within 5 minutes")
	})
}
