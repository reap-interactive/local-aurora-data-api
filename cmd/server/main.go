package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/reap-interactive/local-aurora-data-api/internal/api"
	"github.com/reap-interactive/local-aurora-data-api/internal/dataapi"
)

type config struct {
	port         string
	postgresHost string
	postgresPort string
	postgresUser string
	postgresPass string
	postgresDB   string
}

func loadConfig() config {
	return config{
		port:         getEnv("PORT", "8080"),
		postgresHost: getEnv("POSTGRES_HOST", "127.0.0.1"),
		postgresPort: getEnv("POSTGRES_PORT", "5432"),
		postgresUser: getEnv("POSTGRES_USER", "postgres"),
		postgresPass: getEnv("POSTGRES_PASSWORD", "example"),
		postgresDB:   getEnv("POSTGRES_DB", "postgres"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	cfg := loadConfig()

	db, err := dataapi.OpenPostgresDB(cfg.postgresHost, cfg.postgresPort, cfg.postgresUser, cfg.postgresPass, cfg.postgresDB)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}

	h := api.NewHandler(db, dataapi.NewTransactionStore())

	addr := fmt.Sprintf(":%s", cfg.port)
	log.Printf("rds-data-api-local listening on %s", addr)
	if err := http.ListenAndServe(addr, api.NewServer(h)); err != nil {
		log.Fatalf("server error: %v", err)
	}
}
