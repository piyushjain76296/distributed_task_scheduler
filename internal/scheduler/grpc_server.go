package scheduler

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/distributed-scheduler/internal/database/db"
	"github.com/distributed-scheduler/internal/grpc/schedulerpb"
	"github.com/distributed-scheduler/internal/task"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type GRPCServer struct {
	schedulerpb.UnimplementedSchedulerServiceServer
	pool    *pgxpool.Pool
	queries *db.Queries
	queue   *task.Queue
}

func NewGRPCServer(pool *pgxpool.Pool, queue *task.Queue) *GRPCServer {
	return &GRPCServer{
		pool:    pool,
		queries: db.New(pool),
		queue:   queue,
	}
}

func (s *GRPCServer) RegisterWorker(ctx context.Context, req *schedulerpb.RegisterWorkerRequest) (*schedulerpb.RegisterWorkerResponse, error) {
	slog.Info("Registering worker", "worker_id", req.WorkerId, "hostname", req.Hostname)

	// In a real application, we'd store the supported_task_types correctly as JSON
	_, err := s.queries.RegisterWorker(ctx, db.RegisterWorkerParams{
		ID:                 req.WorkerId,
		Hostname:           req.Hostname,
		Version:            pgtype.Text{String: req.Version, Valid: true},
		MaxConcurrentTasks: req.Capabilities.MaxConcurrentTasks,
	})
	if err != nil {
		slog.Error("Failed to register worker", "error", err)
		return &schedulerpb.RegisterWorkerResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &schedulerpb.RegisterWorkerResponse{Success: true}, nil
}

func (s *GRPCServer) Heartbeat(ctx context.Context, req *schedulerpb.HeartbeatRequest) (*schedulerpb.HeartbeatResponse, error) {
	err := s.queries.UpdateWorkerHeartbeat(ctx, db.UpdateWorkerHeartbeatParams{
		ID:          req.WorkerId,
		ActiveTasks: req.ActiveTasks,
	})
	if err != nil {
		return nil, fmt.Errorf("heartbeat failed: %w", err)
	}
	return &schedulerpb.HeartbeatResponse{Success: true}, nil
}

func (s *GRPCServer) StreamTasks(stream schedulerpb.SchedulerService_StreamTasksServer) error {
	// The client connects and is ready to receive tasks.
	// For simplicity, we assume the first message from client could identify it, 
	// but currently StreamTasks expects ExecuteTaskResponse from client.
	// In a production system, we'd probably require an initial registration message in the stream or use grpc metadata for worker_id.
	// Let's use grpc metadata or expect the client to continuously read.

	ctx := stream.Context()

	// A real implementation would have the worker ID in context metadata
	workerID := "unknown-worker" // Placeholder until metadata extraction is added

	// Start a goroutine to read task results from the worker
	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				slog.Error("Stream read error", "error", err)
				return
			}
			
			// Process task result
			status := "FAILED"
			if res.Status == schedulerpb.ExecuteTaskResponse_STATUS_SUCCEEDED {
				status = "SUCCEEDED"
			}
			
			var execID pgtype.UUID
			if err := execID.Scan(res.TaskId); err != nil {
				slog.Error("Invalid task UUID from worker", "error", err)
				continue
			}

			err = s.queue.ReportTaskResult(ctx, execID, workerID, status, res.ErrorMessage)
			if err != nil {
				slog.Error("Failed to report task result", "error", err)
			}
		}
	}()

	// Polling loop to find tasks for this worker
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			taskExec, err := s.queue.ClaimTask(ctx, workerID, 30*time.Minute)
			if err != nil {
				// No task available or claim failed, just wait
				continue
			}

			// We claimed a task! Send it to the worker
			req := &schedulerpb.ExecuteTaskRequest{
				TaskId:         taskExec.ID.String(),
				WorkflowId:     "", // We'd join this from DB in a full implementation
				ExecutionId:    taskExec.WorkflowExecutionID.String(),
				TaskType:       "generic", // Would be fetched from workflow_tasks table
				Attempt:        taskExec.Attempt,
				TimeoutSeconds: 1800,
			}

			if err := stream.Send(req); err != nil {
				slog.Error("Failed to send task to worker", "error", err)
				// If we fail to send, the lease will eventually expire and another worker will pick it up
				return err
			}
		}
	}
}
