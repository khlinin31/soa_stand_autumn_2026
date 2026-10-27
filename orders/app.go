package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Бюджет ответа на POST /orders — 1 с по требованию. Ждём результат
// обработки не дольше responseBudget от начала запроса, остаток — запас на
// запись ответа и сеть.
const responseBudget = 600 * time.Millisecond

type config struct {
	paymentURL   string
	inventoryURL string
	eventsURL    string
}

type app struct {
	cfg    config
	db     *pgxpool.Pool
	client *http.Client

	payBreaker *breaker
	paySlots   chan struct{} // ограничение одновременных запросов к Payment

	inflight sync.Map // id заказа → struct{}: заказ сейчас обрабатывается в этом процессе

	publishNow chan struct{} // сигнал публикатору outbox: появилось новое событие
}

func newApp(cfg config, db *pgxpool.Pool) *app {
	tr := &http.Transport{
		DialContext:         (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:        200,
		MaxIdleConnsPerHost: 200,
		IdleConnTimeout:     90 * time.Second,
	}
	return &app{
		cfg:        cfg,
		db:         db,
		client:     &http.Client{Transport: tr}, // таймауты — через context у каждого запроса
		payBreaker: newBreaker(12, time.Second, 8*time.Second),
		paySlots:   make(chan struct{}, 10),
		publishNow: make(chan struct{}, 1),
	}
}

type Item struct {
	SKU        string `json:"sku"`
	Qty        int    `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

type orderRequest struct {
	UserID string `json:"user_id"`
	Items  []Item `json:"items"`
}

// POST /orders.
func (a *app) handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	var req orderRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request", "message": "тело не разобрано"})
		return
	}
	amount, ok := validate(req)
	if !ok {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "bad_request",
			"message": "нужны user_id и items: sku, qty >= 1, price_cents >= 0"})
		return
	}

	o := order{ID: newID(), UserID: req.UserID, Status: "pending", AmountCents: amount, Items: req.Items}

	// Сначала — запись в базу. Пока заказа нет в базе, ни Inventory, ни
	// Payment о нём не знают; после записи заказ уже не потеряется.
	a.inflight.Store(o.ID, struct{}{})
	dbCtx, cancel := context.WithTimeout(r.Context(), 600*time.Millisecond)
	err := a.insertOrder(dbCtx, o)
	cancel()
	if err != nil {
		a.inflight.Delete(o.ID)
		slog.Error("POST /orders: запись заказа", "err", err)
		// Без записи в базу заказ принять нельзя: обещать покупателю нечего.
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "unavailable"})
		return
	}

	// Обработка — в отдельной горутине с собственным контекстом: она не
	// прерывается, когда мы ответим покупателю pending.
	done := make(chan string, 1)
	go func() {
		defer a.inflight.Delete(o.ID)
		done <- a.processOrder(o.ID)
	}()

	status := "pending"
	wait := responseBudget - time.Since(start)
	if wait > 0 {
		t := time.NewTimer(wait)
		select {
		case status = <-done:
		case <-t.C:
		}
		t.Stop()
	}
	slog.Info("POST /orders", "id", o.ID, "status", status, "ms", time.Since(start).Milliseconds())
	writeJSON(w, http.StatusCreated, map[string]string{"id": o.ID, "status": status})
}

// GET /orders/{id}.
func (a *app) handleGetOrder(w http.ResponseWriter, r *http.Request) {
	var (
		id, status string
		amount     int64
	)
	err := a.db.QueryRow(r.Context(), `SELECT id, status, amount_cents FROM orders WHERE id = $1`,
		r.PathValue("id")).Scan(&id, &status, &amount)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not_found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": status, "amount_cents": amount})
}

func validate(r orderRequest) (int64, bool) {
	if r.UserID == "" || len(r.Items) == 0 {
		return 0, false
	}
	var amount int64
	for _, it := range r.Items {
		if it.SKU == "" || it.Qty < 1 || it.PriceCents < 0 {
			return 0, false
		}
		if it.PriceCents > 0 && int64(it.Qty) > (math.MaxInt64-amount)/it.PriceCents {
			return 0, false // переполнение суммы
		}
		amount += int64(it.Qty) * it.PriceCents
	}
	return amount, true
}

// callResult — исход HTTP-вызова соседа.
type callResult struct {
	code    int
	timeout bool // не дождались ответа за отведённое время
	err     error
}

func (c callResult) transportErr() bool { return c.err != nil }

func (a *app) call(ctx context.Context, method, url string, body any, headers map[string]string, timeout time.Duration) callResult {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return callResult{err: err}
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rd)
	if err != nil {
		return callResult{err: err}
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return callResult{err: err, timeout: errors.Is(err, context.DeadlineExceeded) || isTimeout(err)}
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	return callResult{code: resp.StatusCode}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// backoff — экспоненциальная пауза base·2^n с потолком и случайным разбросом ±20 %.
func backoff(n int, base, max time.Duration) time.Duration {
	if n < 0 {
		n = 0
	}
	if n > 16 {
		n = 16
	}
	d := base << n
	if d > max || d <= 0 {
		d = max
	}
	return jitter(d)
}

func jitter(d time.Duration) time.Duration {
	f := 0.8 + 0.4*mrand.Float64()
	return time.Duration(float64(d) * f)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
