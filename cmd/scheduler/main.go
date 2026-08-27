package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/distributed-scheduler/internal/grpc/schedulerpb"
	"github.com/distributed-scheduler/internal/scheduler"
	"github.com/distributed-scheduler/internal/task"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
)

func main() {
	slog.Info("Starting Distributed Task Scheduler")

	// Connect to database
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://scheduler:scheduler_password@localhost:5432/distributed_scheduler"
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		slog.Error("Unable to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	// Initialize components
	queue := task.NewQueue(pool)
	server := scheduler.NewGRPCServer(pool, queue)
	sweeper := scheduler.NewSweeper(pool)

	// Start background sweeper
	go sweeper.Start(ctx)

	// Start gRPC server
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		slog.Error("Failed to listen", "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	schedulerpb.RegisterSchedulerServiceServer(grpcServer, server)

	go func() {
		slog.Info("gRPC server listening on :50051")
		if err := grpcServer.Serve(lis); err != nil {
			slog.Error("gRPC server failed", "error", err)
		}
	}()

	// Wait for termination signal
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	slog.Info("Shutting down scheduler gracefully...")
	grpcServer.GracefulStop()
}
