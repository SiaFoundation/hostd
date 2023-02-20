package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"

	"go.sia.tech/core/types"
	"go.sia.tech/core/wallet"
	"go.sia.tech/hostd/build"
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
	apiPassword := os.Getenv("HOSTD_API_PASSWORD")
	if apiPassword != "" {
		fmt.Println("Using HOSTD_API_PASSWORD environment variable.")
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
	phrase := os.Getenv("HOSTD_WALLET_SEED")
	if phrase != "" {
		fmt.Println("Using HOSTD_WALLET_SEED environment variable")
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
	flag.StringVar(&gatewayAddr, "addr", ":0", "address to listen on for peer connections")
	flag.StringVar(&rhp2Addr, "rhp2", ":9982", "address to listen on for RHP2 connections")
	flag.StringVar(&rhp3Addr, "rhp3", ":9983", "address to listen on for RHP3 connections")
	flag.StringVar(&apiAddr, "http", "localhost:9980", "address to serve API on")
	flag.StringVar(&dir, "dir", ".", "directory to store hostd metadata")
	flag.BoolVar(&bootstrap, "bootstrap", true, "bootstrap the gateway and consensus modules")
	flag.BoolVar(&logStdOut, "stdout", false, "log to stdout instead of file")
	flag.StringVar(&logLevel, "loglevel", "warn", "log level (debug, info, warn, error)")
	flag.Parse()

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

	log.Println("hostd", build.Version())
	if flag.Arg(0) == "version" {
		log.Println("Commit:", build.GitRevision())
		log.Println("Build Date:", build.Date())
		return
	}

	// apiPassword := getAPIPassword()
	walletKey := getWalletKey()

	node, err := newNode(gatewayAddr, rhp2Addr, rhp3Addr, dir, bootstrap, walletKey, logger)
	if err != nil {
		log.Fatal(err)
	}
	defer node.Close()

	log.Println("p2p: Listening on", node.g.Address())
	log.Println("host public key:", walletKey)

	l, err := net.Listen("tcp", apiAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()
	/*log.Println("api: Listening on", l.Addr())
	go startWeb(l, n, apiPassword)*/

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt)
	<-signalCh
	log.Println("Shutting down...")
}
