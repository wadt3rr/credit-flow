package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wadt3rr/credit-flow/services/application/config"
	"github.com/wadt3rr/credit-flow/services/application/internal/infrastructure/kafka"
	"github.com/wadt3rr/credit-flow/services/application/internal/infrastructure/postgres"
	"github.com/wadt3rr/credit-flow/services/application/internal/outbox"
	repo "github.com/wadt3rr/credit-flow/services/application/internal/repository/postgres"
)

func main() {
	if err := run(); err != nil {
		slog.Error("application stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("./config/outbox-publisher.yaml")
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	psqlCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	pool, err := postgres.NewPool(psqlCtx, cfg.Postgres)
	if err != nil {
		return fmt.Errorf("create pool: %w", err)
	}
	defer pool.Close()

	repository := repo.NewOutboxRepository(pool)

	producer, err := kafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic)
	if err != nil {
		return fmt.Errorf("create producer: %w", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			slog.Error("close kafka producer", "error", err)
		}
	}()

	publisher, err := outbox.NewOutboxPublisher(repository, producer)
	if err != nil {
		return fmt.Errorf("create outbox publisher: %w", err)
	}

	if err := publisher.Run(ctx, cfg.Outbox.PollInterval); err != nil {
		return fmt.Errorf("run outbox publisher: %w", err)
	}
	return nil
}
