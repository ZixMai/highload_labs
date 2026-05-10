package main

import (
	"context"
	"golang-trino/internal/app"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	appCtx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	app.Run(appCtx, cancel)
}
