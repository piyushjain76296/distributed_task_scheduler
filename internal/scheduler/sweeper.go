package scheduler

import (
	"context"
	"log/slog"
	"time"

	"github.com/distributed-scheduler/internal/database/db"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Sweeper struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

func NewSweeper(pool *pgxpool.Pool) *Sweeper {
	return &Sweeper{
		pool:    pool,
		queries: db.New(pool),
	}
}

func (s *Sweeper) Start(ctx context.Context) {
	slog.Info("Starting Scheduler Sweeper")

	workerTicker := time.NewTicker(10 * time.Second)
	retryTicker := time.NewTicker(5 * time.Second)
	defer workerTicker.Stop()
	defer retryTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("Stopping Scheduler Sweeper")
			return
		case <-workerTicker.C:
			s.sweepStaleWorkers(ctx)
		case <-retryTicker.C:
			s.sweepRetries(ctx)
		}
	}
}

func (s *Sweeper) sweepStaleWorkers(ctx context.Context) {
	// A worker is stale if it hasn't heartbeated in 30 seconds
	staleThreshold := pgtype.Timestamptz{Time: time.Now().Add(-30 * time.Second), Valid: true}
	err := s.queries.MarkStaleWorkersOffline(ctx, staleThreshold)
	if err != nil {
		slog.Error("Failed to mark stale workers offline", "error", err)
	}
}

func (s *Sweeper) sweepRetries(ctx context.Context) {
	// Tasks that have hit their retry_wait time should be queued again
	tasks, err := s.queries.ScheduleRetries(ctx)
	if err != nil {
		slog.Error("Failed to schedule retries", "error", err)
		return
	}
	
	if len(tasks) > 0 {
		slog.Info("Scheduled tasks for retry", "count", len(tasks))
	}
}
