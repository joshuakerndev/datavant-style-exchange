package outbox

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	outboxDBTimeout      = 5 * time.Second
	outboxPublishTimeout = 5 * time.Second
	outboxPollInterval   = 1 * time.Second
	outboxBatchSize      = 100
)

type Publisher struct {
	db               *sql.DB
	kafkaWriter      *kafka.Writer
	logger           *slog.Logger
	pollInterval     time.Duration
	onPublishFailure func()
}

func NewPublisher(db *sql.DB, kafkaWriter *kafka.Writer, logger *slog.Logger, onPublishFailure func()) *Publisher {
	return &Publisher{
		db:               db,
		kafkaWriter:      kafkaWriter,
		logger:           logger,
		pollInterval:     outboxPollInterval,
		onPublishFailure: onPublishFailure,
	}
}

func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		if err := p.publishBatch(ctx); err != nil {
			p.logger.Error("outbox publish failed", "error", err)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

type outboxEvent struct {
	ID        int64
	Topic     string
	Key       string
	Payload   []byte
	CreatedAt time.Time
}

func (p *Publisher) publishBatch(ctx context.Context) error {
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	dbCtx, cancel := context.WithTimeout(ctx, outboxDBTimeout)
	defer cancel()

	rows, err := tx.QueryContext(dbCtx,
		`SELECT id, topic, key, payload, created_at
		 FROM outbox_events
		 WHERE published_at IS NULL
		 ORDER BY created_at
		 LIMIT $1
		 FOR UPDATE SKIP LOCKED`,
		outboxBatchSize,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	var events []outboxEvent
	for rows.Next() {
		var event outboxEvent
		if err := rows.Scan(&event.ID, &event.Topic, &event.Key, &event.Payload, &event.CreatedAt); err != nil {
			return err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	if len(events) == 0 {
		return tx.Commit()
	}

	for _, event := range events {
		publishCtx, publishCancel := context.WithTimeout(ctx, outboxPublishTimeout)
		err := p.kafkaWriter.WriteMessages(publishCtx, kafka.Message{
			Topic: event.Topic,
			Key:   []byte(event.Key),
			Value: event.Payload,
			Time:  event.CreatedAt,
		})
		publishCancel()
		if err != nil {
			p.logger.Error("failed to publish outbox event", "error", err, "event_id", event.ID, "topic", event.Topic, "key", event.Key)
			if p.onPublishFailure != nil {
				p.onPublishFailure()
			}
			continue
		}

		updateCtx, updateCancel := context.WithTimeout(ctx, outboxDBTimeout)
		_, err = tx.ExecContext(updateCtx,
			"UPDATE outbox_events SET published_at = now() WHERE id = $1",
			event.ID,
		)
		updateCancel()
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}
