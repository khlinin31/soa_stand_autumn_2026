package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

const busTimeout = 2 * time.Second

// outboxPublisher — фоновый цикл публикации событий из outbox в шину.
//
// Порядок: POST /events → получили 202 → отметили published_at. Если процесс
// упадёт между публикацией и отметкой, после рестарта событие уйдёт ещё раз
// с тем же id — это допустимо (доставка «как минимум один раз»), подписчик
// отличит копию по id. Потерять событие нельзя: строка в outbox появилась в
// одной транзакции с переводом заказа в paid и удаляется только отметкой.
func (a *app) outboxPublisher(ctx context.Context) {
	var inflight sync.Map
	sem := make(chan struct{}, 32)
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		case <-a.publishNow:
		}
		batch, err := a.dueEvents(ctx, 100)
		if err != nil {
			slog.Error("outbox: выборка", "err", err)
			continue
		}
		for _, ev := range batch {
			if _, busy := inflight.LoadOrStore(ev.id, struct{}{}); busy {
				continue
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			go func(ev outboxEvent) {
				defer func() { <-sem; inflight.Delete(ev.id) }()
				a.publish(ctx, ev)
			}(ev)
		}
	}
}

type outboxEvent struct {
	id       string
	payload  json.RawMessage
	attempts int
}

func (a *app) dueEvents(ctx context.Context, limit int) ([]outboxEvent, error) {
	rows, err := a.db.Query(ctx,
		`SELECT event_id, payload, attempts FROM outbox
		 WHERE published_at IS NULL AND next_attempt_at <= now()
		 ORDER BY created_at LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (outboxEvent, error) {
		var e outboxEvent
		err := r.Scan(&e.id, &e.payload, &e.attempts)
		return e, err
	})
}

func (a *app) publish(ctx context.Context, ev outboxEvent) {
	res := a.call(ctx, http.MethodPost, a.cfg.eventsURL+"/events", ev.payload, nil, busTimeout)
	if res.err == nil && res.code/100 == 2 {
		if _, err := a.db.Exec(ctx,
			`UPDATE outbox SET published_at = now(), attempts = attempts + 1 WHERE event_id = $1`, ev.id); err != nil {
			slog.Error("outbox: отметка публикации", "event", ev.id, "err", err)
		}
		return
	}
	cause := res.err
	if cause == nil {
		cause = fmt.Errorf("шина: ответ %d", res.code)
	}
	delay := backoff(ev.attempts, 250*time.Millisecond, 5*time.Second)
	if _, err := a.db.Exec(ctx,
		`UPDATE outbox SET attempts = attempts + 1, next_attempt_at = now() + $2::interval WHERE event_id = $1`,
		ev.id, fmt.Sprintf("%d milliseconds", delay.Milliseconds())); err != nil {
		slog.Error("outbox: планирование повтора", "event", ev.id, "err", err)
	}
	slog.Warn("outbox: событие не принято, повтор", "event", ev.id, "attempt", ev.attempts+1, "err", cause)
}

type deliveryConfirmed struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	UserID      string `json:"user_id"`
	BonusPoints int64  `json:"bonus_points"`
}

// POST /webhooks/delivery — входящее delivery.confirmed.
//
// Inbox и баланс меняются в одной транзакции:
//
//	INSERT INTO inbox ... ON CONFLICT DO NOTHING
//	  вставилась строка → событие новое → баланс += bonus_points
//	  конфликт          → событие уже обработано → ничего не делаем
//
// Одновременные копии одного события: вторая вставка ждёт на первичном
// ключе, пока первая транзакция завершится, и получает конфликт. Если
// процесс убили до COMMIT, транзакция откатится целиком, шина не получит 2xx
// и пришлёт событие снова. Начисление ровно одно в любом случае.
func (a *app) handleDeliveryWebhook(w http.ResponseWriter, r *http.Request) {
	var ev deliveryConfirmed
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&ev); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	if ev.ID == "" || ev.UserID == "" || ev.BonusPoints < 1 ||
		(ev.Type != "" && ev.Type != "delivery.confirmed") {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request"})
		return
	}
	applied, err := a.applyBonus(r.Context(), ev)
	if err != nil {
		slog.Error("webhook: начисление", "event", ev.ID, "err", err)
		// Шина повторит доставку: 2xx отдаём только после COMMIT.
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "unavailable"})
		return
	}
	result := "duplicate"
	if applied {
		result = "applied"
	}
	slog.Info("webhook delivery.confirmed", "event", ev.ID, "user", ev.UserID, "points", ev.BonusPoints, "result", result)
	writeJSON(w, http.StatusOK, map[string]string{"status": result})
}

func (a *app) applyBonus(ctx context.Context, ev deliveryConfirmed) (bool, error) {
	tx, err := a.db.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx,
		`INSERT INTO inbox (event_id, user_id, bonus_points) VALUES ($1, $2, $3)
		 ON CONFLICT (event_id) DO NOTHING`, ev.ID, ev.UserID, ev.BonusPoints)
	if err != nil {
		return false, err
	}
	if tag.RowsAffected() == 0 {
		return false, tx.Commit(ctx)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO bonus_balances (user_id, points) VALUES ($1, $2)
		 ON CONFLICT (user_id) DO UPDATE SET points = bonus_balances.points + EXCLUDED.points`,
		ev.UserID, ev.BonusPoints); err != nil {
		return false, err
	}
	return true, tx.Commit(ctx)
}

// GET /users/{id}/bonus.
func (a *app) handleGetBonus(w http.ResponseWriter, r *http.Request) {
	var points int64
	err := a.db.QueryRow(r.Context(), `SELECT points FROM bonus_balances WHERE user_id = $1`,
		r.PathValue("id")).Scan(&points)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		slog.Error("bonus: чтение", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"points": points})
}
