package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

const (
	inventoryTimeout = 3 * time.Second
	// Таймаут оплаты растёт с номером попытки: медленный, но живой Payment
	// (S3) успевает ответить, а запрос не висит вечно, если Payment пропал.
	paymentTimeoutMin = 5 * time.Second
	paymentTimeoutMax = 15 * time.Second
	// Сколько горутина ждёт свободный слот к Payment, прежде чем отложить заказ.
	paySlotWait = 3 * time.Second

	lookupTimeout = 2 * time.Second

	// Веса сбоев для breaker (порог — в newApp): два таймаута подряд
	// размыкают его сразу, быстрые 500 — только длинной серией.
	weightTimeout   = 6
	weightTransport = 3
	weight5xx       = 1
)

var (
	errBreakerOpen = errors.New("payment: breaker open")
	errNoPaySlot   = errors.New("payment: нет свободного слота")
)

// processOrder — один проход обработки заказа: резерв → оплата → итог.
// Каждый шаг идемпотентен, поэтому проход можно безопасно повторять сколько
// угодно раз, в том числе после падения процесса посреди шага.
// Возвращает статус заказа после прохода.
func (a *app) processOrder(id string) string {
	ctx := context.Background()
	o, err := a.loadOrder(ctx, id)
	if err != nil {
		slog.Error("process: чтение заказа", "id", id, "err", err)
		return "pending"
	}
	if o.Status != "pending" {
		return o.Status
	}

	// 1. Резерв. Inventory по контракту возвращает уже действующий резерв
	//    (200), поэтому повтор безопасен; флаг reserved экономит запросы.
	if !o.Reserved {
		if err := a.reserve(ctx, o); err != nil {
			slog.Warn("process: резерв не получен", "id", id, "err", err)
			a.scheduleRetry(ctx, id, err, true)
			return "pending"
		}
		if err := a.markReserved(ctx, id); err != nil {
			slog.Error("process: отметка резерва", "id", id, "err", err)
		}
	}

	// Бесплатный заказ: списывать нечего (Payment не принимает сумму 0).
	if o.AmountCents == 0 {
		return a.finishPaid(ctx, id)
	}

	// 2. Оплата.
	res, err := a.charge(ctx, o)
	switch {
	case errors.Is(err, errBreakerOpen), errors.Is(err, errNoPaySlot):
		a.scheduleRetry(ctx, id, err, false)
		return "pending"
	case err != nil:
		slog.Warn("process: оплата без ответа", "id", id, "attempt", o.Attempts+1, "err", err)
		a.scheduleRetry(ctx, id, err, true)
		return "pending"
	}

	switch res.code {
	case http.StatusCreated, http.StatusOK:
		// 201 — списали сейчас; 200 — списание по этому ключу уже было
		// (например, ответ на первый запрос потерялся). Итог один: paid.
		return a.finishPaid(ctx, id)

	case http.StatusPaymentRequired:
		// 3. Банк отказал: снять резерв, потом rejected. Если снять не
		//    удалось, заказ остаётся pending и проход повторится: повторный
		//    запрос с тем же ключом снова вернёт 402, а DELETE идемпотентен.
		if err := a.release(ctx, id); err != nil {
			slog.Warn("process: резерв не снят", "id", id, "err", err)
			a.scheduleRetry(ctx, id, err, true)
			return "pending"
		}
		if err := a.markRejected(ctx, id); err != nil {
			slog.Error("process: запись rejected", "id", id, "err", err)
			a.scheduleRetry(ctx, id, err, true)
			return "pending"
		}
		slog.Info("заказ отклонён банком", "id", id)
		return "rejected"

	default:
		// 400 и 422 при неизменном теле и ключе не ожидаются; заказ остаётся
		// pending, повторы идут с растущей паузой.
		err := fmt.Errorf("payment: неожиданный ответ %d", res.code)
		slog.Error("process", "id", id, "err", err)
		a.scheduleRetry(ctx, id, err, true)
		return "pending"
	}
}

func (a *app) finishPaid(ctx context.Context, id string) string {
	changed, err := a.markPaid(ctx, id)
	if err != nil {
		// Деньги списаны, а статус не записан: заказ остаётся pending,
		// повтор оплаты с тем же ключом вернёт 200 — и запись повторится.
		slog.Error("process: запись paid", "id", id, "err", err)
		a.scheduleRetry(ctx, id, err, true)
		return "pending"
	}
	if changed {
		slog.Info("заказ оплачен", "id", id)
		select {
		case a.publishNow <- struct{}{}:
		default:
		}
	}
	return "paid"
}

func (a *app) reserve(ctx context.Context, o order) error {
	type resvItem struct {
		SKU string `json:"sku"`
		Qty int    `json:"qty"`
	}
	ri := make([]resvItem, 0, len(o.Items))
	for _, it := range o.Items {
		ri = append(ri, resvItem{it.SKU, it.Qty})
	}
	res := a.call(ctx, http.MethodPost, a.cfg.inventoryURL+"/reservations",
		map[string]any{"order_id": o.ID, "items": ri}, nil, inventoryTimeout)
	if res.err != nil {
		return res.err
	}
	if res.code != http.StatusOK && res.code != http.StatusCreated {
		return fmt.Errorf("inventory: ответ %d", res.code)
	}
	return nil
}

func (a *app) release(ctx context.Context, id string) error {
	res := a.call(ctx, http.MethodDelete, a.cfg.inventoryURL+"/reservations/"+id, nil, nil, inventoryTimeout)
	if res.err != nil {
		return res.err
	}
	if res.code/100 != 2 {
		return fmt.Errorf("inventory: ответ %d", res.code)
	}
	return nil
}

// charge — один запрос на списание с Idempotency-Key = id заказа.
// Возвращает ошибку, если ответа по существу нет (breaker, таймаут, разрыв, 5xx).
func (a *app) charge(ctx context.Context, o order) (callResult, error) {
	if !a.payBreaker.Ready() {
		return callResult{}, errBreakerOpen
	}
	// Слот: не больше N одновременных запросов к Payment.
	t := time.NewTimer(paySlotWait)
	select {
	case a.paySlots <- struct{}{}:
		t.Stop()
	case <-t.C:
		return callResult{}, errNoPaySlot
	}
	defer func() { <-a.paySlots }()

	// Решение breaker'а — после получения слота: пока ждали, он мог разомкнуться.
	if !a.payBreaker.Allow() {
		return callResult{}, errBreakerOpen
	}

	timeout := paymentTimeoutMin + time.Duration(o.Attempts)*2*time.Second
	if timeout > paymentTimeoutMax {
		timeout = paymentTimeoutMax
	}
	res := a.call(ctx, http.MethodPost, a.cfg.paymentURL+"/payments",
		map[string]any{"order_id": o.ID, "amount_cents": o.AmountCents, "currency": "RUB"},
		map[string]string{"Idempotency-Key": o.ID}, timeout)

	switch {
	case res.timeout:
		a.payBreaker.Failure(weightTimeout)
		return res, fmt.Errorf("payment: нет ответа за %s", timeout)
	case res.transportErr():
		// Соединение оборвалось без ответа: списание могло пройти. Сверяемся
		// с Payment через GET /payments?order_id= — если списание есть, заказ
		// оплачен, а Payment, значит, жив.
		if found, err := a.lookupCharge(ctx, o.ID); err == nil && found {
			a.payBreaker.Success()
			slog.Info("payment: ответ потерян, списание подтверждено сверкой", "id", o.ID)
			return callResult{code: http.StatusOK}, nil
		}
		a.payBreaker.Failure(weightTransport)
		return res, fmt.Errorf("payment: %w", res.err)
	case res.code >= 500:
		a.payBreaker.Failure(weight5xx)
		return res, fmt.Errorf("payment: ответ %d", res.code)
	default:
		a.payBreaker.Success()
		return res, nil
	}
}

// lookupCharge — есть ли у Payment списание по заказу (GET /payments?order_id=).
// Только чтение: повтор ничего не меняет.
func (a *app) lookupCharge(ctx context.Context, id string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, lookupTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		a.cfg.paymentURL+"/payments?order_id="+url.QueryEscape(id), nil)
	if err != nil {
		return false, err
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return false, fmt.Errorf("payment lookup: ответ %d", resp.StatusCode)
	}
	var charges []struct {
		OrderID string `json:"order_id"`
		Status  string `json:"status"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&charges); err != nil {
		return false, err
	}
	for _, c := range charges {
		if c.OrderID == id && c.Status == "captured" {
			return true, nil
		}
	}
	return false, nil
}

// orderWorker — фоновый цикл: доводит pending-заказы до paid или rejected.
func (a *app) orderWorker(ctx context.Context) {
	sem := make(chan struct{}, 32)
	tick := time.NewTicker(200 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		ids, err := a.duePending(ctx, 64)
		if err != nil {
			slog.Error("worker: выборка", "err", err)
			continue
		}
		for _, id := range ids {
			if _, busy := a.inflight.LoadOrStore(id, struct{}{}); busy {
				continue
			}
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				a.inflight.Delete(id)
				return
			}
			go func(id string) {
				defer func() { <-sem; a.inflight.Delete(id) }()
				a.processOrder(id)
			}(id)
		}
	}
}
