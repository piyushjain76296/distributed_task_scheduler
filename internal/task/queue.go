package task

import (
	"context"
	"fmt"
	"time"

	"github.com/distributed-scheduler/internal/database/db"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Queue struct {
	queries *db.Queries
	pool    *pgxpool.Pool
}

func NewQueue(pool *pgxpool.Pool) *Queue {
	return &Queue{
		queries: db.New(pool),
		pool:    pool,
	}
}

// ClaimTask attempts to atomically claim a task from the QUEUED or expired ASSIGNED state
func (q *Queue) ClaimTask(ctx context.Context, workerID string, leaseDuration time.Duration) (*db.TaskExecution, error) {
	leaseExpires := time.Now().Add(leaseDuration)

	// We use the ClaimTask query which has the strict `FOR UPDATE SKIP LOCKED`
	task, err := q.queries.ClaimTask(ctx, db.ClaimTaskParams{
		WorkerID:       pgtype.Text{String: workerID, Valid: true},
		LeaseExpiresAt: pgtype.Timestamptz{Time: leaseExpires, Valid: true},
	})
	if err != nil {
		return nil, fmt.Errorf("no task available or claim failed: %w", err)
	}

	return &task, nil
}

// ReportTaskResult is called when a worker finishes a task (success or failure)
func (q *Queue) ReportTaskResult(ctx context.Context, taskExecID pgtype.UUID, workerID string, status string, errorMsg string) error {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	qtx := q.queries.WithTx(tx)

	now := pgtype.Timestamptz{Time: time.Now(), Valid: true}
	
	// Record the attempt
	// We need to fetch the attempt number first, or let postgres trigger it.
	// For simplicity, we just insert the attempt log using a subquery or by fetching the task first.
	// But our UpdateTaskExecutionStatus doesn't return the attempt.
	// Since we are in a transaction, let's just update the status.
	
	exec, err := qtx.UpdateTaskExecutionStatus(ctx, db.UpdateTaskExecutionStatusParams{
		ID:          taskExecID,
		Status:      status,
		CompletedAt: now,
	})
	if err != nil {
		return fmt.Errorf("failed to update task execution: %w", err)
	}

	_, err = qtx.RecordTaskAttempt(ctx, db.RecordTaskAttemptParams{
		TaskExecutionID: taskExecID,
		WorkerID:        workerID,
		Attempt:         exec.Attempt,
		Status:          status,
		ErrorMessage:    pgtype.Text{String: errorMsg, Valid: errorMsg != ""},
		CompletedAt:     now,
	})
	if err != nil {
		return fmt.Errorf("failed to record task attempt: %w", err)
	}

	return tx.Commit(ctx)
}
