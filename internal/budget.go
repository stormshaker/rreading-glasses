package internal

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// ErrDailyBudgetExhausted is returned instead of making an upstream request
// once the day's request budget has been spent. It is deliberately not an
// HTTP status error, so retry logic does not keep knocking on a closed door.
var ErrDailyBudgetExhausted = errors.New("daily upstream request budget exhausted")

// budgetStore is the subset of the cache the budget needs to persist its
// count, so a restart part-way through a day does not reset it.
type budgetStore interface {
	Get(ctx context.Context, key string) ([]byte, bool)
	Set(ctx context.Context, key string, val []byte, ttl time.Duration)
}

// DailyBudget caps the number of upstream HTTP requests per UTC day.
//
// Hardcover's free tier allows a fixed number of requests per day on top of
// its per-minute rate. Retries are real requests too, so the budget is
// enforced at the transport layer where every attempt is counted.
type DailyBudget struct {
	limit int
	store budgetStore
	now   func() time.Time

	mu     sync.Mutex
	day    string
	used   int
	warned bool
}

// NewDailyBudget returns a budget of limit requests per UTC day. A limit of
// zero or less means unlimited. store may be nil, in which case the count
// lives only in memory.
func NewDailyBudget(limit int, store budgetStore) *DailyBudget {
	return &DailyBudget{limit: limit, store: store, now: time.Now}
}

func budgetKey(day string) string { return "rg:budget:" + day }

// Take spends one request from today's budget. It reports false, and spends
// nothing, once the budget is exhausted.
func (b *DailyBudget) Take(ctx context.Context) bool {
	if b == nil || b.limit <= 0 {
		return true
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	day := b.now().UTC().Format("2006-01-02")
	if day != b.day {
		b.day, b.used, b.warned = day, 0, false
		if b.store != nil {
			if raw, ok := b.store.Get(ctx, budgetKey(day)); ok {
				if n, err := strconv.Atoi(string(raw)); err == nil {
					b.used = n
				}
			}
		}
	}

	if b.used >= b.limit {
		if !b.warned {
			Log(ctx).Warn("daily upstream request budget exhausted; serving from cache until the next UTC day",
				"limit", b.limit, "day", day)
			b.warned = true
		}
		return false
	}

	b.used++
	if b.store != nil {
		b.store.Set(ctx, budgetKey(day), []byte(strconv.Itoa(b.used)), 48*time.Hour)
	}
	return true
}

// Used reports how many requests have been spent today.
func (b *DailyBudget) Used() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used
}

// BudgetTransport refuses requests once the daily budget is spent.
type BudgetTransport struct {
	Budget *DailyBudget
	http.RoundTripper
}

// RoundTrip counts the request against the budget before sending it.
func (t BudgetTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if !t.Budget.Take(r.Context()) {
		return nil, fmt.Errorf("%s %s: %w", r.Method, r.URL.Host, ErrDailyBudgetExhausted)
	}
	return t.RoundTripper.RoundTrip(r)
}
