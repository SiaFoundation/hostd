package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"go.sia.tech/core/wallet"
	"go.sia.tech/hostd/api"
	"go.sia.tech/hostd/build"
	"go.sia.tech/jape"
	"go.sia.tech/web/hostd"
	"go.uber.org/zap"
	"golang.org/x/term"
	"gopkg.in/yaml.v3"
)

type (
	httpCfg struct {
		Address  string `yaml:"address"`
		Password string `yaml:"password"`
	}

	consensusCfg struct {
		GatewayAddress string   `yaml:"gatewayAddress"`
		Bootstrap      bool     `yaml:"bootstrap"`
		Peers          []string `toml:"peers,omitempty"`
	}

	rhp2Cfg struct {
		Address string `yaml:"address"`
	}

	rhp3Cfg struct {
		TCPAddress       string `yaml:"tcp"`
		WebSocketAddress string `yaml:"websocket"`
		CertPath         string `yaml:"certPath"`
		KeyPath          string `yaml:"keyPath"`
	}

	logCfg struct {
		Path   string `yaml:"path"`
		Level  string `yaml:"level"`
		Stdout bool   `yaml:"stdout"`
	}

	cfg struct {
		Name           string `yaml:"name"`
		DataDir        string `yaml:"dataDir"`
		RecoveryPhrase string `yaml:"recoveryPhrase"`

		HTTP      httpCfg      `yaml:"http"`
		Consensus consensusCfg `yaml:"consensus"`
		RHP2      rhp2Cfg      `yaml:"rhp2"`
		RHP3      rhp3Cfg      `yaml:"rhp3"`
		Log       logCfg       `yaml:"log"`
	}
)

var (
	config = cfg{
		DataDir:        ".",                              // default to current directory
		RecoveryPhrase: os.Getenv(walletSeedEnvVariable), // default to env variable

		HTTP: httpCfg{
			Address:  defaultAPIAddr,
			Password: os.Getenv(apiPasswordEnvVariable),
		},
		Consensus: consensusCfg{
			GatewayAddress: defaultGatewayAddr,
			Bootstrap:      true,
		},
		RHP2: rhp2Cfg{
			Address: defaultRHPv2Addr,
		},
		RHP3: rhp3Cfg{
			TCPAddress:       defaultRHPv3TCPAddr,
			WebSocketAddress: defaultRHPv3WSAddr,
		},

		Log: logCfg{
			Level: "info",
			Path:  os.Getenv(logPathEnvVariable),
		},
	}

	disableStdin bool
)

func check(context string, err error) {
	if err != nil {
		log.Fatalf("%v: %v", context, err)
	}
}

// mustSetAPIPassword prompts the user to enter an API password if one is not
// already set via environment variable or config file.
func mustSetAPIPassword() {
	if len(config.HTTP.Password) != 0 {
		return
	} else if disableStdin {
		log.Fatalln("API password must be set via environment variable or config file when --env flag is set")
	}

	fmt.Print("Enter API password: ")
	pw, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		log.Fatal(err)
	} else if len(pw) == 0 {
		log.Fatalln("API password cannot be empty")
	}
	config.HTTP.Password = string(pw)
}

// mustSetWalletkey prompts the user to enter a wallet seed phrase if one is not
// already set via environment variable or config file.
func mustSetWalletkey() {
	if len(config.RecoveryPhrase) != 0 {
		return
	} else if disableStdin {
		log.Fatalln("Wallet seed must be set via environment variable or config file when --env flag is set")
	}

	fmt.Print("Enter wallet seed: ")
	pw, err := term.ReadPassword(int(os.Stdin.Fd()))
	check("Could not read seed phrase:", err)
	fmt.Println()
	config.RecoveryPhrase = string(pw)
}

// mustLoadConfig loads the config file specified by the HOSTD_CONFIG_PATH. If
// the config file does not exist, it will not be loaded.
func mustLoadConfig() {
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
		log.Fatal("failed to open config file:", err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)

	if err := dec.Decode(&config); err != nil {
		log.Fatal("failed to decode config file:", err)
	}
}

func main() {
	// attempt to load the config file first, command line flags will override
	// any values set in the config file
	mustLoadConfig()

	// global
	flag.StringVar(&config.Name, "name", config.Name, "a friendly name for the host, only used for display")
	flag.StringVar(&config.DataDir, "dir", config.DataDir, "directory to store hostd metadata")
	flag.BoolVar(&disableStdin, "env", false, "disable stdin prompts for environment variables (default false)")
	// consensus
	flag.StringVar(&config.Consensus.GatewayAddress, "rpc", config.Consensus.GatewayAddress, "address to listen on for peer connections")
	flag.BoolVar(&config.Consensus.Bootstrap, "bootstrap", config.Consensus.Bootstrap, "bootstrap the gateway and consensus modules")
	// rhp
	flag.StringVar(&config.RHP2.Address, "rhp2", config.RHP2.Address, "address to listen on for RHP2 connections")
	flag.StringVar(&config.RHP3.TCPAddress, "rhp3.tcp", config.RHP3.TCPAddress, "address to listen on for TCP RHP3 connections")
	flag.StringVar(&config.RHP3.WebSocketAddress, "rhp3.ws", config.RHP3.WebSocketAddress, "address to listen on for WebSocket RHP3 connections")
	// http
	flag.StringVar(&config.HTTP.Address, "http", config.HTTP.Address, "address to serve API on")
	// log
	flag.StringVar(&config.Log.Level, "log.level", config.Log.Level, "log level (debug, info, warn, error)")
	flag.BoolVar(&config.Log.Stdout, "log.stdout", config.Log.Stdout, "log to stdout (default false)")
	flag.Parse()

	log.Println("hostd", build.Version())
	log.Println("Network", build.NetworkName())
	switch flag.Arg(0) {
	case "version":
		log.Println("Commit:", build.Commit())
		log.Println("Build Date:", build.BuildTime())
		return
	case "seed":
		var seed [32]byte
		phrase := wallet.NewSeedPhrase()
		if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
			log.Fatal(err)
		}
		key := wallet.KeyFromSeed(&seed, 0)
		log.Println("Recovery Phrase:", phrase)
		log.Println("Address", key.PublicKey().StandardAddress())
		return
	}

	// check that the API password and wallet seed are set
	mustSetAPIPassword()
	mustSetWalletkey()

	// set the log path to the data dir if it is not already set note: this
	// musst happen after CLI flags are parsed so that the data directory can be
	// specified via the command line
	if len(config.Log.Path) == 0 {
		config.Log.Path = config.DataDir
	}

	var seed [32]byte
	if err := wallet.SeedFromPhrase(&seed, config.RecoveryPhrase); err != nil {
		log.Fatalln("failed to load wallet:", err)
	}
	walletKey := wallet.KeyFromSeed(&seed, 0)

	if err := os.MkdirAll(config.DataDir, 0700); err != nil {
		log.Fatalln("unable to create config directory:", err)
	}

	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{filepath.Join(config.Log.Path, "hostd.log")}
	if config.Log.Stdout {
		cfg.OutputPaths = append(cfg.OutputPaths, "stdout")
	}
	switch config.Log.Level {
	case "debug":
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	default:
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}
	logger, err := cfg.Build()
	if err != nil {
		log.Fatalln("ERROR: failed to create logger:", err)
	}
	defer logger.Sync()
	if config.Log.Stdout {
		zap.RedirectStdLog(logger.Named("stdlog"))
	}

	apiListener, err := net.Listen("tcp", config.HTTP.Address)
	if err != nil {
		log.Fatal(err)
	}
	defer apiListener.Close()

	rhpv3WSListener, err := net.Listen("tcp", config.RHP3.WebSocketAddress)
	if err != nil {
		log.Fatal(err)
	}
	defer rhpv3WSListener.Close()

	node, hostKey, err := newNode(walletKey, logger)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()

	auth := jape.BasicAuth(config.HTTP.Password)
	web := http.Server{
		Handler: webRouter{
			api: auth(api.NewServer(config.Name, hostKey.PublicKey(), node.a, node.g, node.cm, node.tp, node.contracts, node.storage, node.metrics, node.store, node.settings, node.w, logger.Named("api"))),
			ui:  hostd.Handler(),
		},
		ReadTimeout: 30 * time.Second,
	}
	defer web.Close()

	rhpv3WS := http.Server{
		Handler:     node.rhp3.WebSocketHandler(),
		ReadTimeout: 30 * time.Second,
		TLSConfig:   node.settings.RHP3TLSConfig(),
		ErrorLog:    log.New(io.Discard, "", 0),
	}
	defer rhpv3WS.Close()

	go func() {
		err := rhpv3WS.ServeTLS(rhpv3WSListener, "", "")
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			if config.Log.Stdout {
				logger.Error("failed to serve rhpv3 websocket", zap.Error(err))
				return
			}
			log.Println("ERROR: failed to serve rhpv3 websocket:", err)
		}
	}()

	if config.Log.Stdout {
		logger.Info("hostd started", zap.String("hostKey", hostKey.PublicKey().String()), zap.String("api", apiListener.Addr().String()), zap.String("p2p", string(node.g.Address())), zap.String("rhp2", node.rhp2.LocalAddr()), zap.String("rhp3", node.rhp3.LocalAddr()))
	} else {
		log.Println("api listening on:", apiListener.Addr().String())
		log.Println("p2p listening on:", node.g.Address())
		log.Println("rhp2 listening on:", node.rhp2.LocalAddr())
		log.Println("rhp3 TCP listening on:", node.rhp3.LocalAddr())
		log.Println("rhp3 WebSocket listening on:", rhpv3WSListener.Addr().String())
		log.Println("host public key:", hostKey.PublicKey())
	}

	go func() {
		err := web.Serve(apiListener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			if config.Log.Stdout {
				logger.Error("failed to serve web", zap.Error(err))
				return
			}
			log.Println("ERROR: failed to serve web:", err)
		}
	}()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	if config.Log.Stdout {
		logger.Info("shutdown initiated")
	} else {
		log.Println("Shutting down...")
	}
	time.AfterFunc(5*time.Minute, func() {
		os.Exit(-1)
	})
}
