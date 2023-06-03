package main

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	_ "net/http/pprof"
	"path"
	"path/filepath"
	"strings"
	"time"

	"go.sia.tech/web/hostd/ui"
)

type uiRouter struct {
	fs fs.FS
}

func (cr *uiRouter) serveErrorPage(status int, w http.ResponseWriter, r *http.Request) {
	errorPath := fmt.Sprintf("%d.html", status)

	errorPage, err := cr.fs.Open(errorPath)
	if err != nil {
		http.Error(w, http.StatusText(status), status)
		return
	}
	defer errorPage.Close()

	w.WriteHeader(status)
	io.Copy(w, errorPage)
}

func (cr *uiRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	fp := strings.Trim(r.URL.Path, "/")
	if fp == "" {
		fp = "index.html" // serve index.html at /
	}

	// If the path is a file, it is served directly. If the file does not exist,
	// it will check the path with .html appended.
	//
	// If the path is a directory, it will try to serve index.html file in that
	// directory. If an index.html file does not exist in the directory, it will
	// try to serve index.html file in the parent directory. Does not traverse
	// the filesystem more than one level to enable serving 404 pages.
	tryFiles := []string{
		fp,                              // try the exact name first
		fp + ".html",                    // try the exact name with .html appended (/contracts serves /contracts.html)
		filepath.Join(fp, "index.html"), // try the name as a directory (/volumes serves /volumes/index.html)
		filepath.Join(path.Dir(fp), "index.html"), // try the parent directory, (supports path params e.g. /volumes/:id)
	}

	// try each file in order
	for _, try := range tryFiles {
		f, err := cr.fs.Open(try)
		switch {
		case err == nil:
			defer f.Close()
			http.ServeContent(w, r, try, time.Time{}, f.(io.ReadSeeker))
			return
		case !errors.Is(err, fs.ErrNotExist):
			cr.serveErrorPage(http.StatusInternalServerError, w, r)
			return
		}
	}
	// no matching file found
	cr.serveErrorPage(http.StatusNotFound, w, r)
}

func createUIHandler() http.Handler {
	assets, err := fs.Sub(ui.Assets, "assets")
	if err != nil {
		panic(err)
	}
	return &uiRouter{assets}
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
