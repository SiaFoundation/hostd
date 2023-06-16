package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/core/wallet"
	"go.sia.tech/hostd/api"
	"go.sia.tech/hostd/build"
	"go.sia.tech/jape"
	"go.sia.tech/web/hostd"
	"go.uber.org/zap"
	"golang.org/x/term"
)

var (
	gatewayAddr string
	rhp2Addr    string
	rhp3TCPAddr string
	rhp3WSAddr  string
	apiAddr     string
	dir         string
	bootstrap   bool

	logLevel  string
	logStdout bool

	disableStdin bool
)

func check(context string, err error) {
	if err != nil {
		log.Fatalf("%v: %v", context, err)
	}
}

func getAPIPassword() string {
	apiPassword := os.Getenv(apiPasswordEnvVariable)
	if len(apiPassword) != 0 {
		log.Printf("Using %s environment variable.", apiPasswordEnvVariable)
		return apiPassword
	} else if disableStdin {
		log.Fatalf("%s must be set via environment variable when running in docker.", apiPasswordEnvVariable)
	}

	fmt.Print("Enter API password: ")
	pw, err := term.ReadPassword(int(os.Stdin.Fd()))
	fmt.Println()
	if err != nil {
		log.Fatal(err)
	}
	apiPassword = string(pw)
	return apiPassword
}

func getWalletKey() types.PrivateKey {
	phrase := os.Getenv(walletSeedEnvVariable)
	if len(phrase) != 0 {
		log.Printf("Using %s environment variable.", walletSeedEnvVariable)
	} else if disableStdin {
		log.Fatalf("%s must be set via environment variable when running in docker.", walletSeedEnvVariable)
	} else {
		fmt.Print("Enter wallet seed: ")
		pw, err := term.ReadPassword(int(os.Stdin.Fd()))
		check("Could not read seed phrase:", err)
		fmt.Println()
		phrase = string(pw)
	}
	var seed [32]byte
	if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
		log.Fatal(err)
	}
	return wallet.KeyFromSeed(&seed, 0)
}

func main() {
	flag.StringVar(&gatewayAddr, "rpc", defaultGatewayAddr, "address to listen on for peer connections")
	flag.StringVar(&rhp2Addr, "rhp2", defaultRHPv2Addr, "address to listen on for RHP2 connections")
	flag.StringVar(&rhp3TCPAddr, "rhp3.tcp", defaultRHPv3TCPAddr, "address to listen on for TCP RHP3 connections")
	flag.StringVar(&rhp3WSAddr, "rhp3.ws", defaultRHPv3WSAddr, "address to listen on for WebSocket RHP3 connections")
	flag.StringVar(&apiAddr, "http", defaultAPIAddr, "address to serve API on")
	flag.StringVar(&dir, "dir", ".", "directory to store hostd metadata")
	flag.BoolVar(&bootstrap, "bootstrap", true, "bootstrap the gateway and consensus modules")
	flag.StringVar(&logLevel, "log.level", "info", "log level (debug, info, warn, error)")
	flag.BoolVar(&logStdout, "log.stdout", false, "log to stdout (default false)")
	flag.BoolVar(&disableStdin, "env", false, "disable stdin prompts for environment variables (default false)")
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

	if err := os.MkdirAll(dir, 0700); err != nil {
		log.Fatal(err)
	}

	logPath := dir
	if elp := os.Getenv(logPathEnvVariable); len(elp) != 0 {
		logPath = elp
	}

	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{filepath.Join(logPath, "hostd.log")}
	if logStdout {
		cfg.OutputPaths = append(cfg.OutputPaths, "stdout")
	}
	switch logLevel {
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
	if logStdout {
		zap.RedirectStdLog(logger.Named("stdlog"))
	}

	apiPassword := getAPIPassword()
	walletKey := getWalletKey()

	apiListener, err := net.Listen("tcp", apiAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer apiListener.Close()

	rhpv3WSListener, err := net.Listen("tcp", rhp3WSAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer rhpv3WSListener.Close()

	node, hostKey, err := newNode(gatewayAddr, rhp2Addr, rhp3TCPAddr, dir, bootstrap, walletKey, logger, cfg.Level.Level())
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()

	auth := jape.BasicAuth(apiPassword)
	web := http.Server{
		Handler: webRouter{
			api: auth(api.NewServer(hostKey.PublicKey(), node.a, node.g, node.cm, node.tp, node.contracts, node.storage, node.metrics, node.store, node.settings, node.w, logger.Named("api"))),
			ui:  hostd.Handler(),
		},
		ReadTimeout: 30 * time.Second,
	}
	defer web.Close()

	rhpv3WS := http.Server{
		Handler:     node.rhp3.WebSocketHandler(),
		ReadTimeout: 30 * time.Second,
		TLSConfig:   node.settings.RHP3TLSConfig(),
		ErrorLog:    nil,
	}
	defer rhpv3WS.Close()

	go func() {
		err := rhpv3WS.ServeTLS(rhpv3WSListener, "", "")
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			if logStdout {
				logger.Error("failed to serve rhpv3 websocket", zap.Error(err))
				return
			}
			log.Println("ERROR: failed to serve rhpv3 websocket:", err)
		}
	}()

	if logStdout {
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
			if logStdout {
				logger.Error("failed to serve web", zap.Error(err))
				return
			}
			log.Println("ERROR: failed to serve web:", err)
		}
	}()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	if logStdout {
		logger.Info("shutdown initiated")
	} else {
		log.Println("Shutting down...")
	}
	time.AfterFunc(5*time.Minute, func() {
		os.Exit(-1)
	})
}
