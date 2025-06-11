package explorer

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"

	"go.sia.tech/jape"
)

func TestForexError(t *testing.T) {
	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer l.Close()

	s := &http.Server{
		Handler: jape.Mux(map[string]jape.Handler{
			"GET /exchange-rate/siacoin/:currency": func(jc jape.Context) {
				jc.Error(errors.New("test error"), http.StatusInternalServerError)
			},
		}),
	}
	defer s.Close()
	go func() {
		if err := s.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
			panic(err)
		}
	}()

	e := New("http://" + l.Addr().String())
	if _, err := e.SiacoinExchangeRate(context.Background(), "usd"); !strings.Contains(err.Error(), "test error") {
		t.Fatalf("expected test error, got %v", err)
	}
}
