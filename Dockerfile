# One image, three entrypoints (cmd/api, cmd/worker, cmd/scheduler) plus
# cmd/seed for the release_command - fly.toml's process groups all point
# at this same build, differing only in which binary they run. Building
# once and selecting a command at runtime is simpler to reason about and
# keep in sync than three separate images that could drift apart.

FROM node:24-alpine AS web-builder
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ .
RUN npm run build

FROM golang:1.25-alpine AS go-builder
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# Overwrites the committed placeholder (web/dist/index.html) with the real
# build before web/embed.go's //go:embed directive runs - this is the one
# place in the whole pipeline where the dashboard actually ends up inside
# the API binary.
COPY --from=web-builder /web/dist ./web/dist
# CGO_ENABLED=0: pgx v5 is pure Go, so a static binary is free, and it's
# what makes the tiny alpine final stage possible without dragging along
# libc compatibility concerns.
RUN CGO_ENABLED=0 go build -o /out/jq-api ./cmd/api
RUN CGO_ENABLED=0 go build -o /out/jq-worker ./cmd/worker
RUN CGO_ENABLED=0 go build -o /out/jq-scheduler ./cmd/scheduler
RUN CGO_ENABLED=0 go build -o /out/jq-seed ./cmd/seed

FROM alpine:3.20
# ca-certificates: DATABASE_URL points at an external managed Postgres
# (Neon/Supabase) over TLS in production - without this, every connection
# attempt fails certificate verification.
RUN apk add --no-cache ca-certificates \
    && addgroup -S jobqueue && adduser -S jobqueue -G jobqueue
COPY --from=go-builder /out/jq-api /out/jq-worker /out/jq-scheduler /out/jq-seed /usr/local/bin/
USER jobqueue
