// Orders — сервис оформления заказов маркетплейса.
//
// Устройство (подробно — в README.md рядом):
//
//   - POST /orders сначала записывает заказ в базу со статусом pending и
//     только потом обращается к Inventory и Payment. Деньги не могут быть
//     списаны за заказ, которого нет в базе, даже если процесс убили.
//   - Оплата идёт с заголовком Idempotency-Key = id заказа. Повтор запроса
//     (после 500, таймаута, потерянного ответа, перезапуска) не создаёт
//     второго списания: Payment возвращает результат первой операции.
//   - Обработку заказа ведёт отдельная горутина. HTTP-ответ ждёт её не дольше
//     бюджета и отвечает тем статусом, который известен к этому моменту;
//     pending дорабатывает фоновый воркер.
//   - Circuit breaker ограничивает запросы к Payment, пока тот недоступен.
//   - Событие order.paid пишется в таблицу outbox в одной транзакции со
//     сменой статуса на paid и публикуется в шину отдельным циклом до 202.
//   - Входящие delivery.confirmed дедуплицируются по id события в таблице
//     inbox в одной транзакции с начислением бонусов.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	cfg := config{
		paymentURL:   strings.TrimRight(mustEnv("PAYMENT_URL"), "/"),
		inventoryURL: strings.TrimRight(mustEnv("INVENTORY_URL"), "/"),
		eventsURL:    strings.TrimRight(os.Getenv("EVENTS_URL"), "/"),
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := connect(ctx, mustEnv("DATABASE_URL"))
	if err != nil {
		slog.Error("база недоступна", "err", err)
		os.Exit(1)
	}
	defer db.Close()

	a := newApp(cfg, db)
	if err := a.recoverAfterRestart(ctx); err != nil {
		slog.Error("восстановление после перезапуска", "err", err)
		os.Exit(1)
	}

	go a.orderWorker(ctx)
	if cfg.eventsURL != "" {
		go a.outboxPublisher(ctx)
	} else {
		slog.Warn("EVENTS_URL не задан: публикация событий выключена")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /orders", a.handleCreateOrder)
	mux.HandleFunc("GET /orders/{id}", a.handleGetOrder)
	mux.HandleFunc("POST /webhooks/delivery", a.handleDeliveryWebhook)
	mux.HandleFunc("GET /users/{id}/bonus", a.handleGetBonus)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	srv := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shCtx)
	}()

	slog.Info("orders: слушаю", "port", port)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

func mustEnv(k string) string {
	v := os.Getenv(k)
	if v == "" {
		slog.Error("не задана переменная окружения", "name", k)
		os.Exit(1)
	}
	return v
}
