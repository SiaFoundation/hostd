package main

import (
	"errors"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"go.sia.tech/web/hostd/ui"
)

type clientRouterFS struct {
	fs fs.FS
}

func (cr *clientRouterFS) Open(name string) (fs.File, error) {
	f, err := cr.fs.Open(name)
	if errors.Is(err, fs.ErrNotExist) {
		return cr.fs.Open("index.html")
	}
	return f, err
}

func createUIHandler() http.Handler {
	assets, err := fs.Sub(ui.Assets, "assets")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(&clientRouterFS{fs: assets}))
}

type webRouter struct {
	ui  http.Handler
	api http.Handler
}

func (wr webRouter) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch {
	case strings.HasPrefix(req.URL.Path, "/api"):
		req.URL.Path = strings.TrimPrefix(req.URL.Path, "/api") // strip the prefix
		wr.api.ServeHTTP(w, req)
	case strings.HasPrefix(req.URL.Path, "/debug/pprof"):
		http.DefaultServeMux.ServeHTTP(w, req)
	default:
		wr.ui.ServeHTTP(w, req)
	}
}
