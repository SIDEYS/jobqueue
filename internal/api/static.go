package api

import (
	"io/fs"
	"net/http"
	"strings"

	"github.com/SIDEYS/jobqueue/web"
)

// dashboardFS is web.DistFS rooted at "dist" so requested paths (/,
// /assets/index-XXXX.js) match the embedded tree directly instead of
// needing a "dist/" prefix stripped on every lookup.
var dashboardFS = mustSub(web.DistFS, "dist")

func mustSub(f fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(f, dir)
	if err != nil {
		// Only possible if web/embed.go's directive or this path drifts
		// out of sync with each other - a build-time programming error,
		// not a runtime condition to handle gracefully.
		panic(err)
	}
	return sub
}

// dashboard serves the built dashboard for everything that isn't the REST
// API, health checks, or metrics (see router.go - this is registered as
// the catch-all). Falls back to index.html for any path with no matching
// file, standard single-page-app behavior so a direct navigation or
// refresh on a client-side route doesn't 404 - there are none of those
// yet (the dashboard is tab-based, no router), but this makes adding one
// later free.
func (a *API) dashboard() http.HandlerFunc {
	fileServer := http.FileServer(http.FS(dashboardFS))
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path != "" {
			if _, err := fs.Stat(dashboardFS, path); err != nil {
				r = r.Clone(r.Context())
				r.URL.Path = "/"
			}
		}
		fileServer.ServeHTTP(w, r)
	}
}
