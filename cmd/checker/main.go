package main

import (
	"os"

	"go.sia.tech/hostd/persist/sqlite"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	cfg := zap.NewProductionEncoderConfig()
	cfg.TimeKey = "" // prevent duplicate timestamps
	cfg.EncodeTime = zapcore.RFC3339TimeEncoder
	cfg.EncodeDuration = zapcore.StringDurationEncoder
	cfg.EncodeLevel = zapcore.CapitalColorLevelEncoder

	cfg.StacktraceKey = ""
	cfg.CallerKey = ""
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(cfg), zapcore.Lock(os.Stdout), zap.NewAtomicLevelAt(zapcore.DebugLevel))
	log := zap.New(core, zap.AddCaller())
	defer log.Sync()

	db, err := sqlite.OpenDatabase(os.Args[1], log.Named("sqlite3"))
	if err != nil {
		log.Fatal("failed to open database", zap.Error(err))
	}
	defer db.Close()

	if err := db.VerifyAccountFunding(); err != nil {
		log.Fatal("failed to verify account funding", zap.Error(err))
	}
}
