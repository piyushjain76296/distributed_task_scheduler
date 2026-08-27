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
	
	if status == "FAILED" {
		// Determine if we should retry or move to dead letter
		exec, err := qtx.FailTaskExecution(ctx, taskExecID)
		if err != nil {
			return fmt.Errorf("failed to fail task execution: %w", err)
		}
		
		if exec.Attempt < exec.MaxAttempts {
			// Exponential backoff logic (simplified to fixed 5s for demo)
			nextRetry := time.Now().Add(5 * time.Second)
			_, err = qtx.MarkTaskForRetry(ctx, db.MarkTaskForRetryParams{
				ID:          taskExecID,
				NextRetryAt: pgtype.Timestamptz{Time: nextRetry, Valid: true},
			})
			if err != nil {
				return fmt.Errorf("failed to mark for retry: %w", err)
			}
		} else {
			// Move to dead letter queue
			_, err = qtx.MoveToDeadLetter(ctx, db.MoveToDeadLetterParams{
				TaskExecutionID: taskExecID,
				LastError:       pgtype.Text{String: errorMsg, Valid: errorMsg != ""},
			})
			if err != nil {
				return fmt.Errorf("failed to move to dead letter queue: %w", err)
			}
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

	} else {
		// Success
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
	}

	return tx.Commit(ctx)
}
