package larkcache

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"time"
)

func New(clearInterval time.Duration) *Cache {
	cache := &Cache{}
	cache.cron = newCron(cache.clear, clearInterval)
	runtime.SetFinalizer(cache, stopCron)

	return cache
}

type Cache struct {
	m    sync.Map
	cron *cron
}

type elem struct {
	val    interface{}
	expire time.Time
}

type cron struct {
	cmd      func()
	interval time.Duration
	stop     chan struct{}
	done     chan struct{}
	once     sync.Once
}

func (c *cron) start() {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()
	defer close(c.done)
	for {
		select {
		case <-ticker.C:
			c.cmd()
		case <-c.stop:
			return
		}
	}
}

func newCron(cmd func(), interval time.Duration) *cron {
	cron := &cron{
		cmd:      cmd,
		interval: interval,
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}

	go cron.start()

	return cron
}

func stopCron(c *Cache) { c.requestStop() }

func (c *Cache) requestStop() {
	if c == nil || c.cron == nil {
		return
	}
	c.cron.once.Do(func() {
		close(c.cron.stop)
		runtime.SetFinalizer(c, nil)
	})
}

// Close stops and joins the cleaner. Concurrent calls are safe. A deadline
// reports incomplete joining, but still requests that the cleaner stop.
// Closing does not erase cached values or change Get/Set expiry semantics.
func (c *Cache) Close(ctx context.Context) error {
	if ctx == nil {
		return errors.New("cache: nil context")
	}
	if c == nil || c.cron == nil {
		return nil
	}
	c.requestStop()
	select {
	case <-c.cron.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *Cache) Get(key string) interface{} {
	if val, ok := c.m.Load(key); ok {
		elem := val.(*elem)
		if elem.expire.After(time.Now()) {
			return elem.val
		}
	}
	return nil
}

func (c *Cache) Set(key string, value interface{}, expire time.Duration) {
	expireTime := time.Now().Add(expire)
	c.m.Store(key, &elem{
		val:    value,
		expire: expireTime,
	})
}

func (c *Cache) clear() {
	c.m.Range(func(k, v interface{}) bool {
		elem := v.(*elem)
		if elem.expire.Before(time.Now()) {
			c.m.Delete(k)
		}
		return true
	})
}
