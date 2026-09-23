// Package web embeds the built dashboard (dist/, produced by `npm run
// build`) so cmd/api can serve it directly with no separate static host.
//
// dist/ ships a committed placeholder index.html (see .gitignore) so this
// directive has something to embed even before the frontend has ever been
// built - go build ./... must work out of the box on a fresh clone. The
// Dockerfile overwrites dist/ with the real build before the Go build
// stage runs; a local go run ./cmd/api without npm run build first just
// serves the placeholder instead of the real dashboard.
package web

import "embed"

//go:embed dist
var DistFS embed.FS
