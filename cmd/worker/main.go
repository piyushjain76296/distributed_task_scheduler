package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/distributed-scheduler/internal/worker"
)

func main() {
	workerID := flag.String("id", "worker-1", "Unique worker ID")
	schedulerAddr := flag.String("scheduler", "localhost:50051", "Scheduler gRPC address")
	flag.Parse()

	hostname, _ := os.Hostname()
	w, err := worker.NewWorker(*workerID, hostname, "1.0.0", *schedulerAddr)
	if err != nil {
		slog.Error("Failed to initialize worker", "error", err)
		os.Exit(1)
	}
	defer w.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := w.Start(ctx); err != nil {
			slog.Error("Worker failed", "error", err)
			os.Exit(1)
		}
	}()

	// Wait for termination signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	slog.Info("Shutting down worker gracefully...")
	cancel() // Stop the context loop
}
