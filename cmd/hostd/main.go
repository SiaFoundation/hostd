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
	"go.uber.org/zap"
	"golang.org/x/term"
)

var (
	gatewayAddr string
	rhp2Addr    string
	rhp3Addr    string
	apiAddr     string
	dir         string
	bootstrap   bool

	logLevel  string
	logStdOut bool
)

func check(context string, err error) {
	if err != nil {
		log.Fatalf("%v: %v", context, err)
	}
}

func getAPIPassword() string {
	apiPassword := os.Getenv(apiPasswordEnvVariable)
	if apiPassword != "" {
		log.Printf("Using %s environment variable.", apiPasswordEnvVariable)
	} else {
		fmt.Print("Enter API password: ")
		pw, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Println()
		if err != nil {
			log.Fatal(err)
		}
		apiPassword = string(pw)
	}
	return apiPassword
}

func getWalletKey() types.PrivateKey {
	phrase := os.Getenv(walletSeedEnvVariable)
	if phrase != "" {
		log.Printf("Using %s environment variable", walletSeedEnvVariable)
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
	flag.StringVar(&gatewayAddr, "addr", defaultGatewayAddr, "address to listen on for peer connections")
	flag.StringVar(&rhp2Addr, "rhp2", defaultRHPv2Addr, "address to listen on for RHP2 connections")
	flag.StringVar(&rhp3Addr, "rhp3", defaultRHPv3Addr, "address to listen on for RHP3 connections")
	flag.StringVar(&apiAddr, "http", defaultAPIAddr, "address to serve API on")
	flag.StringVar(&dir, "dir", ".", "directory to store hostd metadata")
	flag.BoolVar(&bootstrap, "bootstrap", true, "bootstrap the gateway and consensus modules")
	flag.BoolVar(&logStdOut, "log.stdout", false, "log to stdout instead of file")
	flag.StringVar(&logLevel, "log.level", "warn", "log level (debug, info, warn, error)")
	flag.Parse()

	log.Println("hostd", build.Version())
	switch flag.Arg(0) {
	case "version":
		log.Println("Commit:", build.GitRevision())
		log.Println("Build Date:", build.Date())
		return
	case "seed":
		var seed [32]byte
		phrase := wallet.NewSeedPhrase()
		if err := wallet.SeedFromPhrase(&seed, phrase); err != nil {
			log.Fatal(err)
		}
		key := wallet.KeyFromSeed(&seed, 0)
		address := wallet.StandardAddress(key.PublicKey())
		log.Println("Recovery Phrase:", phrase)
		log.Println("Address", address)
		return
	}

	cfg := zap.NewProductionConfig()
	if logStdOut {
		cfg.OutputPaths = []string{"stdout"}
	} else {
		cfg.OutputPaths = []string{filepath.Join(dir, "hostd.log")}
	}
	switch logLevel {
	case "debug":
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	default:
		cfg.Level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	}
	logger, err := cfg.Build()
	if err != nil {
		log.Fatalln("ERROR: failed to create logger:", err)
	}
	defer logger.Sync()

	apiPassword := getAPIPassword()
	walletKey := getWalletKey()

	node, err := newNode(gatewayAddr, rhp2Addr, rhp3Addr, dir, bootstrap, walletKey, logger)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()

	log.Println("p2p listening on:", node.g.Address())
	log.Println("rhp2 listening on:", node.rhp2.LocalAddr())
	log.Println("rhp3 listening on:", node.rhp3.LocalAddr())
	log.Println("host public key:", walletKey.PublicKey())

	l, err := net.Listen("tcp", apiAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	auth := jape.BasicAuth(apiPassword)
	web := http.Server{
		Handler: webRouter{
			api: auth(api.NewServer(node.g, node.contracts, node.storage, node.settings, node.w, logger.Named("api"))),
			ui:  createUIHandler(),
		},
		ReadTimeout: 30 * time.Second,
	}

	go func() {
		err := web.Serve(l)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Println("ERROR: failed to serve web:", err)
		}
	}()
	defer web.Close()

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	<-signalCh
	log.Println("Shutting down...")
	time.AfterFunc(5*time.Minute, func() {
		os.Exit(-1)
	})
}
