# ── Build stage ───────────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

WORKDIR /app

# Download dependencies first so this layer is cached unless go.mod/go.sum change.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically-linked binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o rds-data-api-local \
    ./cmd/server

# ── Runtime stage ─────────────────────────────────────────────────────────────
# distroless/static:nonroot includes ca-certificates and tzdata; runs as UID 65532.
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /app/rds-data-api-local /app/rds-data-api-local

EXPOSE 8080

CMD ["/app/rds-data-api-local"]
