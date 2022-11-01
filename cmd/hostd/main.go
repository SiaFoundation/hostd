package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"

	"go.sia.tech/hostd/build"
	"go.sia.tech/hostd/wallet"
	"golang.org/x/term"
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

func getWalletKey() ed25519.PrivateKey {
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
	key, err := wallet.KeyFromPhrase(phrase)
	if err != nil {
		log.Fatal(err)
	}
	return key
}

func main() {
	log.SetFlags(0)
	gatewayAddr := flag.String("addr", ":0", "address to listen on for peer connections")
	rhp2Addr := flag.String("rhp2", ":9982", "address to listen on for RHP2 connections")
	rhp3Addr := flag.String("rhp3", ":9983", "address to listen on for RHP3 connections")
	apiAddr := flag.String("http", "localhost:9980", "address to serve API on")
	dir := flag.String("dir", ".", "directory to store node state in")
	bootstrap := flag.Bool("bootstrap", true, "bootstrap the gateway and consensus modules")
	flag.Parse()

	log.Println("hostd", build.Version())
	if flag.Arg(0) == "version" {
		log.Println("Commit:", build.GitRevision())
		log.Println("Build Date:", build.BuildDate())
		return
	}

	// apiPassword := getAPIPassword()
	walletKey := getWalletKey()
	n, err := newNode(*gatewayAddr, *rhp2Addr, *rhp3Addr, *dir, *bootstrap, walletKey)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := n.Close(); err != nil {
			log.Println("WARN: error shutting down:", err)
		}
	}()
	log.Println("p2p: Listening on", n.g.Address())
	log.Println("host public key:", "ed25519:"+hex.EncodeToString(walletKey.Public().(ed25519.PublicKey)))

	l, err := net.Listen("tcp", *apiAddr)
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
