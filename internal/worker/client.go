package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/distributed-scheduler/internal/grpc/schedulerpb"
	"google.golang.org/grpc"
)

type Worker struct {
	id         string
	hostname   string
	version    string
	client     schedulerpb.SchedulerServiceClient
	conn       *grpc.ClientConn
	activeTask int32
}

func NewWorker(id, hostname, version, schedulerAddr string) (*Worker, error) {
	conn, err := grpc.Dial(schedulerAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to scheduler: %w", err)
	}

	return &Worker{
		id:       id,
		hostname: hostname,
		version:  version,
		client:   schedulerpb.NewSchedulerServiceClient(conn),
		conn:     conn,
	}, nil
}

func (w *Worker) Start(ctx context.Context) error {
	// 1. Register with scheduler
	resp, err := w.client.RegisterWorker(ctx, &schedulerpb.RegisterWorkerRequest{
		WorkerId: w.id,
		Hostname: w.hostname,
		Version:  w.version,
		Capabilities: &schedulerpb.WorkerCapabilities{
			MaxConcurrentTasks: 10,
			SupportedTaskTypes: []string{"generic"},
		},
	})
	if err != nil {
		return fmt.Errorf("registration failed: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("registration rejected: %s", resp.ErrorMessage)
	}
	slog.Info("Successfully registered with scheduler")

	// 2. Start heartbeat loop
	go w.heartbeatLoop(ctx)

	// 3. Connect task stream
	stream, err := w.client.StreamTasks(ctx)
	if err != nil {
		return fmt.Errorf("failed to open task stream: %w", err)
	}

	slog.Info("Listening for tasks...")
	for {
		req, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("stream closed or error: %w", err)
		}

		slog.Info("Received task", "task_id", req.TaskId, "type", req.TaskType)

		// Simulate execution
		err = w.executeTask(ctx, req)
		
		status := schedulerpb.ExecuteTaskResponse_STATUS_SUCCEEDED
		errMsg := ""
		if err != nil {
			status = schedulerpb.ExecuteTaskResponse_STATUS_FAILED
			errMsg = err.Error()
		}

		// Report result
		err = stream.Send(&schedulerpb.ExecuteTaskResponse{
			TaskId:       req.TaskId,
			Status:       status,
			ErrorMessage: errMsg,
		})
		if err != nil {
			slog.Error("Failed to send task result", "error", err)
		}
	}
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := w.client.Heartbeat(ctx, &schedulerpb.HeartbeatRequest{
				WorkerId:    w.id,
				ActiveTasks: w.activeTask,
			})
			if err != nil {
				slog.Error("Heartbeat failed", "error", err)
			}
		}
	}
}

func (w *Worker) executeTask(ctx context.Context, req *schedulerpb.ExecuteTaskRequest) error {
	w.activeTask++
	defer func() { w.activeTask-- }()
	
	// Simulate work
	time.Sleep(2 * time.Second)
	return nil
}

func (w *Worker) Close() error {
	return w.conn.Close()
}
