package internal

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memBudgetStore struct {
	mu sync.Mutex
	m  map[string][]byte
}

func (s *memBudgetStore) Get(_ context.Context, key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.m[key]
	return v, ok
}

func (s *memBudgetStore) Set(_ context.Context, key string, val []byte, _ time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[key] = val
}

func TestDailyBudgetStopsAtLimit(t *testing.T) {
	ctx := context.Background()
	b := NewDailyBudget(3, nil)
	for i := 0; i < 3; i++ {
		assert.True(t, b.Take(ctx), "request %d should be allowed", i+1)
	}
	assert.False(t, b.Take(ctx), "fourth request should be refused")
	assert.Equal(t, 3, b.Used())
}

func TestDailyBudgetResetsAtUTCMidnight(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 9, 23, 59, 0, 0, time.UTC)
	b := NewDailyBudget(1, nil)
	b.now = func() time.Time { return now }

	assert.True(t, b.Take(ctx))
	assert.False(t, b.Take(ctx))

	now = now.Add(2 * time.Minute) // 2026-11-10
	assert.True(t, b.Take(ctx), "a new UTC day gets a fresh budget")
}

func TestDailyBudgetSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	store := &memBudgetStore{m: map[string][]byte{}}
	now := func() time.Time { return time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC) }

	first := NewDailyBudget(2, store)
	first.now = now
	assert.True(t, first.Take(ctx))
	assert.True(t, first.Take(ctx))

	// A new process on the same day must pick up the spent count.
	second := NewDailyBudget(2, store)
	second.now = now
	assert.False(t, second.Take(ctx), "a restart must not reset the day's budget")
}

func TestDailyBudgetUnlimited(t *testing.T) {
	ctx := context.Background()
	b := NewDailyBudget(0, nil)
	for i := 0; i < 100; i++ {
		require.True(t, b.Take(ctx))
	}
	var nilBudget *DailyBudget
	assert.True(t, nilBudget.Take(ctx))
}

type countingTransport struct{ n int }

func (c *countingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	c.n++
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody}, nil
}

func TestBudgetTransportRefusesWithoutCallingUpstream(t *testing.T) {
	inner := &countingTransport{}
	tr := BudgetTransport{Budget: NewDailyBudget(1, nil), RoundTripper: inner}

	req, err := http.NewRequest(http.MethodPost, "https://api.hardcover.app/v1/graphql", nil)
	require.NoError(t, err)

	resp, err := tr.RoundTrip(req)
	require.NoError(t, err)
	_ = resp.Body.Close()

	_, err = tr.RoundTrip(req) //nolint:bodyclose // refused, no body
	assert.True(t, errors.Is(err, ErrDailyBudgetExhausted))
	assert.Equal(t, 1, inner.n, "the refused request must never reach upstream")
	assert.NotRegexp(t, `429|5\d\d`, err.Error(), "must not look retryable to the batch client")
}
