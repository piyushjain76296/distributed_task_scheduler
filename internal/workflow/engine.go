package workflow

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/distributed-scheduler/internal/database/db"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Engine struct {
	queries *db.Queries
	pool    *pgxpool.Pool
}

func NewEngine(pool *pgxpool.Pool) *Engine {
	return &Engine{
		queries: db.New(pool),
		pool:    pool,
	}
}

func (e *Engine) SubmitWorkflow(ctx context.Context, tenantID pgtype.UUID, def *Definition) (*db.WorkflowExecution, error) {
	// Start transaction
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	qtx := e.queries.WithTx(tx)

	// 1. Create or get Workflow definition
	defBytes, _ := json.Marshal(def)
	wf, err := qtx.CreateWorkflow(ctx, db.CreateWorkflowParams{
		TenantID:   tenantID,
		Name:       def.Name,
		Definition: defBytes,
		Version:    1,
	})
	if err != nil {
		// In a real app we might handle conflict on name/version
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}

	// 2. Create Workflow Execution
	// Using a simple idempotency key for now (can be passed from caller)
	idempKey := pgtype.Text{String: fmt.Sprintf("%s-exec", wf.ID.String()), Valid: true}
	exec, err := qtx.CreateWorkflowExecution(ctx, db.CreateWorkflowExecutionParams{
		TenantID:       tenantID,
		WorkflowID:     wf.ID,
		IdempotencyKey: idempKey,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create execution: %w", err)
	}

	// 3. Create Workflow Tasks
	for i, task := range def.Tasks {
		deps, _ := json.Marshal(task.DependsOn)
		wTask, err := qtx.CreateWorkflowTask(ctx, db.CreateWorkflowTaskParams{
			WorkflowID:   wf.ID,
			TaskName:     task.ID,
			TaskType:     task.Type,
			Dependencies: deps,
			Priority:     int32(i),
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create workflow task %s: %w", task.ID, err)
		}

		// If a task has no dependencies, it can be scheduled immediately
		if len(task.DependsOn) == 0 {
			_, err = qtx.CreateTaskExecution(ctx, db.CreateTaskExecutionParams{
				WorkflowExecutionID: exec.ID,
				WorkflowTaskID:      wTask.ID,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to create task execution: %w", err)
			}
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit tx: %w", err)
	}

	return &exec, nil
}

func (e *Engine) CompleteTask(ctx context.Context, taskExecID pgtype.UUID, result []byte) error {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin tx: %w", err)
	}
	defer tx.Rollback(ctx)
	qtx := e.queries.WithTx(tx)

	// 1. Mark task as SUCCEEDED
	now := pgtype.Timestamptz{Valid: true} // ideally time.Now()
	_, err = qtx.UpdateTaskExecutionStatus(ctx, db.UpdateTaskExecutionStatusParams{
		ID:          taskExecID,
		Status:      "SUCCEEDED",
		CompletedAt: now,
	})
	if err != nil {
		return fmt.Errorf("failed to update task status: %w", err)
	}

	// 2. Here we would normally check the workflow DAG for dependent tasks
	// that have all their dependencies met, and enqueue them as PENDING.
	// We leave this algorithm for the next phase.

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit tx: %w", err)
	}
	return nil
}
