package main

import (
	"sync"
	"time"
)

// breaker — автомат защиты (circuit breaker) для Payment.
//
//	closed    — запросы идут. Сбои подряд копятся в счётчике с весами,
//	            успешный ответ его обнуляет. Вес отражает, насколько сбой
//	            похож на недоступность сервиса:
//	              таймаут (сервис молчит)            — 6,
//	              разрыв соединения без ответа        — 3,
//	              быстрый ответ 5xx (сервис жив)      — 1.
//	            При пороге 12 автомат размыкается после двух таймаутов подряд,
//	            но не после случайной серии ответов 500.
//	open      — запросы не отправляются до openUntil.
//	half-open — после openUntil пропускается ровно один пробный запрос.
//	            Успех — closed, сбой — снова open с удвоенной паузой (до max).
//
// Ответ 402 («банк отказал») — нормальный ответ сервиса, он считается успехом.
type breaker struct {
	mu        sync.Mutex
	state     int
	score     int
	threshold int
	base, max time.Duration
	pause     time.Duration
	openUntil time.Time
}

const (
	stClosed = iota
	stOpen
	stHalfOpen
)

func newBreaker(threshold int, base, max time.Duration) *breaker {
	return &breaker{threshold: threshold, base: base, max: max, pause: base}
}

// Ready — можно ли сейчас рассчитывать на запрос (ничего не меняет).
// Воркер по нему решает, брать ли заказы в работу.
func (b *breaker) Ready() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stClosed:
		return true
	case stOpen:
		return !time.Now().Before(b.openUntil)
	default:
		return false
	}
}

// Allow — разрешение на один запрос. В half-open выдаётся один раз.
func (b *breaker) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stClosed:
		return true
	case stOpen:
		if time.Now().Before(b.openUntil) {
			return false
		}
		b.state = stHalfOpen
		return true
	default:
		return false
	}
}

// Success — сервис ответил по существу.
func (b *breaker) Success() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state, b.score, b.pause = stClosed, 0, b.base
}

// Failure — сбой: 5xx, разрыв соединения или таймаут (weight 2).
func (b *breaker) Failure(weight int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stClosed:
		b.score += weight
		if b.score >= b.threshold {
			b.state, b.pause = stOpen, b.base
			b.openUntil = time.Now().Add(b.pause)
		}
	case stHalfOpen:
		// Пробный запрос не дождался ответа или соединение оборвалось —
		// пауза удваивается. Быстрый 5xx — сервис отвечает, пауза базовая.
		if weight > 1 {
			b.pause *= 2
			if b.pause > b.max {
				b.pause = b.max
			}
		} else {
			b.pause = b.base
		}
		b.state = stOpen
		b.openUntil = time.Now().Add(b.pause)
	case stOpen:
		// Поздний сбой запроса, отправленного до размыкания: уже учтено.
	}
}

func (b *breaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return [...]string{"closed", "open", "half-open"}[b.state]
}
