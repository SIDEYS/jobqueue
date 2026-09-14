// Package migrations embeds the SQL migration files into the binary so
// cmd/api and cmd/worker can run them on startup without depending on the
// working directory the process happens to be launched from (important
// inside a container, where relative paths to the source tree don't exist).
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
