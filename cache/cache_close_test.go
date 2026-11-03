package larkcache

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestCloseJoinsConcurrentCallsAndPreservesValues(t *testing.T) {
	c := New(time.Hour)
	c.Set("key", "value", time.Hour)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Close(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	select {
	case <-c.cron.done:
	default:
		t.Fatal("cleaner not joined")
	}
	if got := c.Get("key"); got != "value" {
		t.Fatalf("value = %v", got)
	}
	if err := c.Close(nil); err == nil {
		t.Fatal("nil context accepted")
	}
}

func TestCloseDeadlineStillRequestsStop(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	c := &Cache{cron: newCron(func() { once.Do(func() { close(entered) }); <-release }, time.Millisecond)}
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := c.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Close = %v", err)
	}
	close(release)
	if err := c.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.cron.done:
	default:
		t.Fatal("cleaner not joined after deadline")
	}
}
