package main

import (
	"net/http"
	_ "net/http/pprof"
	"strings"
)

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
