package trino_connector

import (
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"

	"github.com/trinodb/trino-go-client/trino"
	_ "github.com/trinodb/trino-go-client/trino"
)

const (
	defaultTrinoHost     = "localhost"
	defaultTrinoPort     = 8080
	defaultTrinoUser     = "trino"
	defaultTrinoPassword = "trino"
	defaultTrinoCatalog  = "postgres"
	defaultTrinoWaitSec  = 90
	defaultTrinoSchema   = "public"
	defaultTrinoScheme   = "http"
)

func ConfigFromEnv() (trino.Config, error) {
	host := getEnv("TRINO_HOST", defaultTrinoHost)
	port, err := getEnvInt("TRINO_PORT", defaultTrinoPort)
	if err != nil {
		return trino.Config{}, err
	}
	user := getEnv("TRINO_USER", defaultTrinoUser)
	password := getEnv("TRINO_PASSWORD", defaultTrinoPassword)
	catalog := getEnv("TRINO_CATALOG", defaultTrinoCatalog)
	schema := getEnv("TRINO_SCHEMA", defaultTrinoSchema)
	scheme := getEnv("TRINO_SCHEME", defaultTrinoScheme)

	serverURL := url.URL{
		Scheme: scheme,
		Host:   fmt.Sprintf("%s:%d", host, port),
	}
	if password != "" {
		serverURL.User = url.UserPassword(user, password)
	} else {
		serverURL.User = url.User(user)
	}

	return trino.Config{
		ServerURI: serverURL.String(),
		Catalog:   catalog,
		Schema:    schema,
	}, nil
}

func GetTrinoClient(cfg trino.Config) (*sql.DB, error) {
	dsn, err := cfg.FormatDSN()
	if err != nil {
		return nil, fmt.Errorf("format trino dsn: %w", err)
	}

	db, err := sql.Open("trino", dsn)
	if err != nil {
		return nil, fmt.Errorf("open trino connection: %w", err)
	}
	if err := waitForTrino(db); err != nil {
		_ = db.Close()
		return nil, err
	}

	return db, nil
}

func waitForTrino(db *sql.DB) error {
	deadline := time.Now().Add(time.Duration(getEnvIntOr(defaultTrinoWaitSec, "TRINO_WAIT_SECONDS")) * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if err := db.Ping(); err != nil {
			lastErr = err
			time.Sleep(2 * time.Second)
			continue
		}

		var one int
		if err := db.QueryRow("SELECT 1").Scan(&one); err == nil {
			return nil
		} else {
			lastErr = err
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("ping trino: %w", lastErr)
}

func getEnv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getEnvInt(key string, fallback int) (int, error) {
	if value := os.Getenv(key); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return 0, fmt.Errorf("parse %s: %w", key, err)
		}
		return parsed, nil
	}
	return fallback, nil
}

func getEnvIntOr(fallback int, key string) int {
	value, err := getEnvInt(key, fallback)
	if err != nil {
		return fallback
	}
	return value
}
