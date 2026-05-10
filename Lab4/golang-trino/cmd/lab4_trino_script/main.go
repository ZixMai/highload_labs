package main

import (
	"context"
	"golang-trino/internal/app"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	appCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer cancel()

	if err := app.Run(appCtx); err != nil {
		os.Exit(1)
	}
}
