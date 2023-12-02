package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"go.sia.tech/hostd/internal/migrate"
	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// read a line of input from stdin
func readInput(context string) (string, error) {
	fmt.Printf("%s: ", context)
	input, err := bufio.NewReader(os.Stdin).ReadString('\n')
	fmt.Println()
	return string(input), err
}

func verifyDirectory(dataPath string) error {
	// check if the directory exists
	if stat, err := os.Stat(dataPath); err != nil {
		return fmt.Errorf("failed to stat directory: %w", err)
	} else if !stat.IsDir() {
		return fmt.Errorf("not a directory: %s", dataPath)
	}

	requiredPaths := []string{
		filepath.Join(dataPath, "consensus", "consensus.db"),
		filepath.Join(dataPath, "host", "host.json"),
		filepath.Join(dataPath, "host", "host.db"),
		filepath.Join(dataPath, "host", "contractmanager", "contractmanager.json"),
	}

	for _, path := range requiredPaths {
		if _, err := os.Stat(path); err != nil {
			return fmt.Errorf("failed to stat required file %q: %w", path, err)
		}
	}
	return nil
}

func main() {
	var logLevel string
	flag.StringVar(&logLevel, "log.level", "info", "set the log level")
	flag.Parse()

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

	// configure logging
	var level zap.AtomicLevel
	switch logLevel {
	case "debug":
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		level = zap.NewAtomicLevelAt(zap.WarnLevel)
	case "error":
		level = zap.NewAtomicLevelAt(zap.ErrorLevel)
	default:
		panic("invalid log level")
	}

	// only log info messages to console unless stdout logging is enabled
	consoleCore := zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), level)
	log := zap.New(consoleCore, zap.AddCaller())
	defer log.Sync()
	// redirect stdlib log to zap
	zap.RedirectStdLog(log)

	fmt.Println("siad Host Migrator")
	fmt.Println("This tool will migrate your old siad host to hostd")
	fmt.Println("It will migrate any existing contracts, storage folders, and settings")
	fmt.Println("It will not migrate your wallet, but you can sweep it to a new hostd seed after migration")
	fmt.Println("")
	fmt.Println("Please make sure your computer will not go to turn off during the migration")
	readInput("Press enter to continue")

	// read the siad directory
	fmt.Println("Where is your siad data directory? Enter the path and press enter to continue.")
	dir, err := readInput("siad data directory")
	if err != nil {
		log.Fatal("failed to read input", zap.Error(err))
	}

	// check to make sure everything is in order
	fmt.Println("Checking directory structure...")
	if err := verifyDirectory(dir); err != nil {
		log.Fatal("failed to verify siad directory structure", zap.Error(err))
	}

	// ask if the migration should be destructive
	var destructive bool
	fmt.Println("Do you want to delete the old data files during migration?")
	fmt.Println("Unless you have double the available disk space, it is required to delete the old data files to free up space for the new storage volumes")
	fmt.Println("This will free up disk space, but you will no longer be able to use siad. If the migration fails, you may be left in a broken state.")

	for {
		answer, err := readInput("delete old data files? (yes/no)")
		if err != nil {
			log.Fatal("failed to read input", zap.Error(err))
		}
		switch answer {
		case "yes":
			destructive = true
		case "no":
			destructive = false
		default:
			fmt.Println("Please enter yes or no")
			fmt.Println("")
			continue
		}
		break
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill, syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	db, err := sqlite.OpenDatabase(filepath.Join(dir, "hostd.db"), log.Named("sqlite3"))
	if err != nil {
		log.Fatal("failed to open database", zap.Error(err))
	}
	defer db.Close()

	log.Info("starting siad migration", zap.String("siadDir", dir), zap.Bool("destructive", destructive))

	start := time.Now()
	if err := migrate.Siad(ctx, db, dir, destructive, log.Named("migrate")); err != nil {
		log.Fatal("siad migration failed", zap.Error(err))
	}

	log.Info("siad migration complete!!!", zap.Duration("elapsed", time.Since(start)))
	log.Info("please start hostd with the same data directory as siad to continue using your host")
	log.Info("you will also need to generate a new 12-word wallet seed and send your balance from the old wallet to the new wallet")
}
