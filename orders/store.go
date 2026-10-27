package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Схема создаётся при старте; база orders в начале прогона пустая.
//
//   - orders: заказы. Pending-заказы с next_attempt_at <= now() забирает
//     воркер; reserved = резерв в Inventory уже получен.
//   - outbox: исходящие события order.paid. Строка появляется в той же
//     транзакции, что и смена статуса заказа на paid; published_at ставится
//     после ответа 202 от шины.
//   - inbox: id уже обработанных входящих событий delivery.confirmed.
//   - bonus_balances: бонусный баланс покупателя; меняется в одной
//     транзакции со вставкой в inbox.
var schema = []string{
	`CREATE TABLE IF NOT EXISTS orders (
		id              TEXT PRIMARY KEY,
		user_id         TEXT NOT NULL,
		status          TEXT NOT NULL CHECK (status IN ('pending', 'paid', 'rejected')),
		amount_cents    BIGINT NOT NULL,
		items           JSONB NOT NULL,
		reserved        BOOLEAN NOT NULL DEFAULT false,
		attempts        INT NOT NULL DEFAULT 0,
		next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		last_error      TEXT,
		created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS orders_pending_idx ON orders (next_attempt_at) WHERE status = 'pending'`,
	`CREATE TABLE IF NOT EXISTS outbox (
		event_id        TEXT PRIMARY KEY,
		order_id        TEXT NOT NULL UNIQUE REFERENCES orders (id),
		payload         JSONB NOT NULL,
		created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
		published_at    TIMESTAMPTZ,
		attempts        INT NOT NULL DEFAULT 0,
		next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS outbox_unpublished_idx ON outbox (next_attempt_at) WHERE published_at IS NULL`,
	`CREATE TABLE IF NOT EXISTS inbox (
		event_id     TEXT PRIMARY KEY,
		user_id      TEXT NOT NULL,
		bonus_points BIGINT NOT NULL,
		received_at  TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`CREATE TABLE IF NOT EXISTS bonus_balances (
		user_id TEXT PRIMARY KEY,
		points  BIGINT NOT NULL DEFAULT 0
	)`,
}

// connect ждёт базу (она может подниматься дольше сервиса) и создаёт схему.
func connect(ctx context.Context, url string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 25
	var lastErr error
	for range 120 {
		db, err := pgxpool.NewWithConfig(ctx, cfg)
		if err == nil {
			if err = migrate(ctx, db); err == nil {
				return db, nil
			}
			db.Close()
		}
		lastErr = err
		slog.Info("база недоступна, жду", "err", err)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return nil, lastErr
}

func migrate(ctx context.Context, db *pgxpool.Pool) error {
	for _, q := range schema {
		if _, err := db.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

type order struct {
	ID          string
	UserID      string
	Status      string
	AmountCents int64
	Items       []Item
	Reserved    bool
	Attempts    int
}

var errNotFound = errors.New("not found")

func (a *app) insertOrder(ctx context.Context, o order) error {
	items, err := json.Marshal(o.Items)
	if err != nil {
		return err
	}
	// next_attempt_at в будущем: пока заказ обрабатывает горутина запроса,
	// воркеру его брать незачем (защита от двойной обработки есть и в памяти).
	_, err = a.db.Exec(ctx,
		`INSERT INTO orders (id, user_id, status, amount_cents, items, next_attempt_at)
		 VALUES ($1, $2, 'pending', $3, $4, now() + interval '3 seconds')`,
		o.ID, o.UserID, o.AmountCents, items)
	return err
}

func (a *app) loadOrder(ctx context.Context, id string) (order, error) {
	var (
		o     order
		items []byte
	)
	err := a.db.QueryRow(ctx,
		`SELECT id, user_id, status, amount_cents, items, reserved, attempts FROM orders WHERE id = $1`, id).
		Scan(&o.ID, &o.UserID, &o.Status, &o.AmountCents, &items, &o.Reserved, &o.Attempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return o, errNotFound
	}
	if err != nil {
		return o, err
	}
	if err := json.Unmarshal(items, &o.Items); err != nil {
		return o, fmt.Errorf("items: %w", err)
	}
	return o, nil
}

func (a *app) markReserved(ctx context.Context, id string) error {
	_, err := a.db.Exec(ctx, `UPDATE orders SET reserved = true, updated_at = now() WHERE id = $1`, id)
	return err
}

// markPaid переводит заказ в paid и в той же транзакции кладёт событие
// order.paid в outbox. Либо есть и статус, и событие, либо ничего.
// Возвращает true, если статус сменил именно этот вызов.
func (a *app) markPaid(ctx context.Context, id string) (bool, error) {
	tx, err := a.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx,
		`UPDATE orders SET status = 'paid', last_error = NULL, updated_at = now()
		 WHERE id = $1 AND status = 'pending'`, id)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, nil // уже не pending: кто-то довёл заказ раньше
	}
	// id события детерминирован: у всех копий одного события он одинаковый.
	evID := "order-paid-" + id
	payload, _ := json.Marshal(map[string]string{
		"id":          evID,
		"type":        "order.paid",
		"order_id":    id,
		"occurred_at": time.Now().UTC().Format(time.RFC3339Nano),
	})
	if _, err := tx.Exec(ctx,
		`INSERT INTO outbox (event_id, order_id, payload) VALUES ($1, $2, $3)
		 ON CONFLICT (event_id) DO NOTHING`, evID, id, payload); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func (a *app) markRejected(ctx context.Context, id string) error {
	_, err := a.db.Exec(ctx,
		`UPDATE orders SET status = 'rejected', last_error = NULL, updated_at = now()
		 WHERE id = $1 AND status = 'pending'`, id)
	return err
}

// scheduleRetry откладывает следующую попытку обработки заказа.
// countAttempt = false — попытка не состоялась (breaker не пустил к Payment),
// счётчик и экспоненциальная пауза не растут.
func (a *app) scheduleRetry(ctx context.Context, id string, cause error, countAttempt bool) {
	var delay time.Duration
	if countAttempt {
		var attempts int
		err := a.db.QueryRow(ctx,
			`UPDATE orders SET attempts = attempts + 1, updated_at = now() WHERE id = $1 RETURNING attempts`, id).
			Scan(&attempts)
		if err != nil {
			slog.Error("retry: счётчик попыток", "id", id, "err", err)
		}
		delay = backoff(attempts, 200*time.Millisecond, 2*time.Second)
	} else {
		delay = jitter(700 * time.Millisecond)
	}
	msg := ""
	if cause != nil {
		msg = cause.Error()
	}
	if _, err := a.db.Exec(ctx,
		`UPDATE orders SET next_attempt_at = now() + $2::interval, last_error = $3, updated_at = now()
		 WHERE id = $1 AND status = 'pending'`,
		id, fmt.Sprintf("%d milliseconds", delay.Milliseconds()), msg); err != nil {
		slog.Error("retry: планирование", "id", id, "err", err)
	}
}

// duePending — pending-заказы, которым пора повторить попытку.
func (a *app) duePending(ctx context.Context, limit int) ([]string, error) {
	rows, err := a.db.Query(ctx,
		`SELECT id FROM orders WHERE status = 'pending' AND next_attempt_at <= now()
		 ORDER BY next_attempt_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// recoverAfterRestart — после старта процесса (в том числе после kill -9)
// все незавершённые заказы и неопубликованные события берутся в работу сразу.
// Копия Orders одна, поэтому чужих «захватов» быть не может.
func (a *app) recoverAfterRestart(ctx context.Context) error {
	tag, err := a.db.Exec(ctx, `UPDATE orders SET next_attempt_at = now() WHERE status = 'pending'`)
	if err != nil {
		return err
	}
	tag2, err := a.db.Exec(ctx, `UPDATE outbox SET next_attempt_at = now() WHERE published_at IS NULL`)
	if err != nil {
		return err
	}
	slog.Info("восстановление после старта", "pending_orders", tag.RowsAffected(), "unpublished_events", tag2.RowsAffected())
	return nil
}
