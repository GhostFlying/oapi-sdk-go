package ws

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func cleanerCount() int {
	b := make([]byte, 2<<20)
	n := runtime.Stack(b, true)
	return strings.Count(string(b[:n]), "cache.(*cron).start(")
}

func TestDisposeBeforeStartJoinsCacheAndIsTerminal(t *testing.T) {
	before := cleanerCount()
	for i := 0; i < 5; i++ {
		c := NewClient("app-id", "app-secret")
		if err := c.Dispose(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := c.Dispose(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := c.Start(context.Background()); !errors.Is(err, errClientTerminal) {
			t.Fatalf("Start = %v", err)
		}
	}
	if got := cleanerCount(); got > before {
		t.Fatalf("cleaners grew: before %d, after %d", before, got)
	}
}

func TestDisposeActiveClientJoinsWorkersAndCache(t *testing.T) {
	gateway, cleanupGateway := newLifecycleGateway(t)
	defer cleanupGateway()
	var requests int32
	domain, cleanupBootstrap := installLifecycleBootstrap(t, gateway.endpoint(), &requests)
	defer cleanupBootstrap()
	ready := make(chan struct{}, 1)
	c := NewClient("app-id", "app-secret", WithDomain(domain), WithAutoReconnect(false), WithOnReady(func() { ready <- struct{}{} }))
	result := startLifecycleClient(c, context.Background())
	waitLifecycleSignal(t, ready, "ready")
	ctx, cancel := context.WithTimeout(context.Background(), lifecycleTestTimeout)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Dispose(ctx); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if err, ok := waitLifecycleResult(result); !ok || err != nil {
		t.Fatalf("Start returned %v, complete %v", err, ok)
	}
	if err := c.Start(context.Background()); !errors.Is(err, errClientTerminal) {
		t.Fatalf("Start = %v", err)
	}
	if got := atomic.LoadInt32(&requests); got != 1 {
		t.Fatalf("requests = %d", got)
	}
}

func TestDisposeDeadlineCanBeJoinedAgain(t *testing.T) {
	gateway, cleanupGateway := newLifecycleGateway(t)
	defer cleanupGateway()
	var requests int32
	domain, cleanupBootstrap := installLifecycleBootstrap(t, gateway.endpoint(), &requests)
	defer cleanupBootstrap()
	ready, entered, release := make(chan struct{}, 1), make(chan struct{}), make(chan struct{})
	c := NewClient("app-id", "app-secret", WithDomain(domain), WithAutoReconnect(false), WithOnReady(func() { ready <- struct{}{} }), WithOnDisconnected(func() { close(entered); <-release }))
	result := startLifecycleClient(c, context.Background())
	waitLifecycleSignal(t, ready, "ready")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	disposed := make(chan error, 1)
	go func() { disposed <- c.Dispose(ctx) }()
	waitLifecycleSignal(t, entered, "disconnected")
	cancel()
	if err, ok := waitLifecycleResult(disposed); !ok || !errors.Is(err, context.Canceled) {
		t.Fatalf("Dispose = %v, complete %v", err, ok)
	}
	close(release)
	if err := c.Dispose(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err, ok := waitLifecycleResult(result); !ok || err != nil {
		t.Fatalf("Start = %v, complete %v", err, ok)
	}
}

func TestDisposeAfterCancelledStartupDoesNotLeaveCleaners(t *testing.T) {
	before := cleanerCount()
	for i := 0; i < 5; i++ {
		c := NewClient("app-id", "app-secret")
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := c.Start(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Start = %v", err)
		}
		if err := c.Dispose(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := cleanerCount(); got > before {
		t.Fatalf("cleaners grew: before %d, after %d", before, got)
	}
}
