package logging

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
)

const flushInterval = 10 * time.Second

type (
	// A Filter filters log entries by name, level, and/or time.
	Filter struct {
		Names   []string        `json:"names"`
		Callers []string        `json:"callers"`
		Levels  []zapcore.Level `json:"levels"`
		Before  time.Time       `json:"before"`
		After   time.Time       `json:"after"`

		Limit  int `json:"limit"`
		Offset int `json:"offset"`
	}

	// An Entry is a log entry.
	Entry struct {
		Timestamp time.Time       `json:"timestamp"`
		Level     zapcore.Level   `json:"level"`
		Name      string          `json:"name"`
		Caller    string          `json:"caller"`
		Message   string          `json:"message"`
		Fields    json.RawMessage `json:"fields"`
	}

	// A LogStore stores log entries.
	LogStore interface {
		AddEntries(entries []Entry) error
		LogEntries(filter Filter) ([]Entry, int, error)
	}

	// A logBuffer buffers log entries and periodically flushes them to the
	// underlying store
	logBuffer struct {
		store LogStore

		mu      sync.Mutex
		entries []Entry
	}

	// A zapCore wraps a LogStore in a zapcore.Core.
	zapCore struct {
		zapcore.LevelEnabler
		encoder zapcore.Encoder
		fields  []zapcore.Field
		buffer  *logBuffer
	}
)

// flush flushes the log entries to the database.
func (lb *logBuffer) flush() error {
	lb.mu.Lock()
	entries := lb.entries
	lb.entries = nil
	lb.mu.Unlock()
	if len(entries) == 0 {
		return nil
	}
	return lb.store.AddEntries(entries)
}

// append appends a log entry to the syncer's buffer. If the buffer has not
// been flushed in a while, it is flushed in a separate goroutine.
func (lb *logBuffer) append(entry Entry) error {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.entries = append(lb.entries, entry)
	return nil
}

// Enabled implements the zapcore.Core interface.
func (zc *zapCore) Enabled(level zapcore.Level) bool {
	return zc.LevelEnabler.Enabled(level)
}

// With implements the zapcore.Core interface.
func (zc *zapCore) With(fields []zapcore.Field) zapcore.Core {
	clone := *zc
	clone.encoder = zc.encoder.Clone()
	clone.fields = append(fields, zc.fields...)
	for _, field := range fields {
		field.AddTo(clone.encoder)
	}
	return &clone
}

// Check implements the zapcore.Core interface.
func (zc *zapCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if zc.Enabled(entry.Level) {
		return checked.AddCore(entry, zc)
	}
	return checked
}

// Write implements the zapcore.Core interface.
func (zc *zapCore) Write(ze zapcore.Entry, zf []zapcore.Field) error {
	buf, err := zc.encoder.EncodeEntry(ze, zf)
	if err != nil {
		return fmt.Errorf("failed to encode field: %w", err)
	}
	entry := Entry{
		Timestamp: ze.Time,
		Level:     ze.Level,
		Name:      ze.LoggerName,
		Message:   ze.Message,
		Fields:    buf.Bytes(),
	}
	if ze.Caller.Defined {
		entry.Caller = ze.Caller.String()
	}
	if err := json.Unmarshal(buf.Bytes(), &entry.Fields); err != nil {
		return fmt.Errorf("failed to parse log entry: %w", err)
	}
	zc.buffer.append(entry)
	return nil
}

// Sync implements the zapcore.Core interface.
func (zc *zapCore) Sync() error {
	return zc.buffer.flush()
}

// Core returns a new zapcore.Core that writes to the given LogStore.
func Core(s LogStore, level zapcore.LevelEnabler) zapcore.Core {
	zc := &zapCore{
		LevelEnabler: level,
		encoder: zapcore.NewJSONEncoder(zapcore.EncoderConfig{
			// setting the encoder keys to empty strings prevents the entry
			// from being encoded
			MessageKey:    "",
			NameKey:       "",
			TimeKey:       "",
			LevelKey:      "",
			CallerKey:     "",
			FunctionKey:   "",
			StacktraceKey: "",
		}),
		buffer: &logBuffer{
			store: s,
		},
	}
	// start a timer that periodically flushes the buffer
	var t *time.Timer
	t = time.AfterFunc(flushInterval, func() {
		zc.buffer.flush()
		t.Reset(flushInterval)
	})
	return zc
}
