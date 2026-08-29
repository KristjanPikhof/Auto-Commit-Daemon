package cli

import (
	"sync"
	"time"
)

const (
	workerActivityHintQuietPeriod = 100 * time.Millisecond
	workerActivityHintMaxTail     = 500 * time.Millisecond
)

type workerActivityHintBurst struct {
	started    time.Time
	generation uint64
	hints      int
	timer      *time.Timer
}

// workerActivityHintCoalescer preserves a leading wake while collapsing a
// burst into one bounded trailing wake. Bursts are independent per worktree.
type workerActivityHintCoalescer struct {
	mu          sync.Mutex
	wakeIdle    *sync.Cond
	wake        func(string)
	quietPeriod time.Duration
	maxTail     time.Duration
	bursts      map[string]*workerActivityHintBurst
	activeWakes int
	closed      bool
}

func newWorkerActivityHintCoalescer(
	quietPeriod, maxTail time.Duration,
	wake func(string),
) *workerActivityHintCoalescer {
	if quietPeriod <= 0 {
		quietPeriod = workerActivityHintQuietPeriod
	}
	if maxTail <= 0 {
		maxTail = workerActivityHintMaxTail
	}
	coalescer := &workerActivityHintCoalescer{
		wake: wake, quietPeriod: quietPeriod, maxTail: maxTail,
		bursts: make(map[string]*workerActivityHintBurst),
	}
	coalescer.wakeIdle = sync.NewCond(&coalescer.mu)
	return coalescer
}

func (c *workerActivityHintCoalescer) Hint(worktreeID string) {
	now := time.Now()
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	burst := c.bursts[worktreeID]
	leading := burst == nil
	if leading {
		burst = &workerActivityHintBurst{started: now}
		c.bursts[worktreeID] = burst
		c.activeWakes++
	}
	burst.hints++
	burst.generation++
	generation := burst.generation
	if burst.timer != nil {
		burst.timer.Stop()
	}
	deadline := now.Add(c.quietPeriod)
	if maxDeadline := burst.started.Add(c.maxTail); deadline.After(maxDeadline) {
		deadline = maxDeadline
	}
	delay := time.Until(deadline)
	if delay < 0 {
		delay = 0
	}
	burst.timer = time.AfterFunc(delay, func() {
		c.fireTrailing(worktreeID, burst, generation)
	})
	c.mu.Unlock()

	if leading {
		c.runWake(worktreeID)
	}
}

func (c *workerActivityHintCoalescer) fireTrailing(
	worktreeID string,
	burst *workerActivityHintBurst,
	generation uint64,
) {
	c.mu.Lock()
	if c.closed || c.bursts[worktreeID] != burst || burst.generation != generation {
		c.mu.Unlock()
		return
	}
	delete(c.bursts, worktreeID)
	if burst.hints < 2 {
		c.mu.Unlock()
		return
	}
	c.activeWakes++
	c.mu.Unlock()
	c.runWake(worktreeID)
}

func (c *workerActivityHintCoalescer) runWake(worktreeID string) {
	defer func() {
		c.mu.Lock()
		c.activeWakes--
		if c.activeWakes == 0 {
			c.wakeIdle.Broadcast()
		}
		c.mu.Unlock()
	}()
	c.wake(worktreeID)
}

func (c *workerActivityHintCoalescer) Close() {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		for worktreeID, burst := range c.bursts {
			burst.timer.Stop()
			delete(c.bursts, worktreeID)
		}
	}
	for c.activeWakes > 0 {
		c.wakeIdle.Wait()
	}
	c.mu.Unlock()
}
