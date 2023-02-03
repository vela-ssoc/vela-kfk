package kfk

import (
	"context"
	"go.uber.org/ratelimit"
	"time"
)

func newLimiter(n, p int) ratelimit.Limiter {
	if n <= 0 {
		return nil
	}

	pre := time.Second
	if p > 0 {
		pre = time.Duration(p) * time.Second
	}

	return ratelimit.New(n, ratelimit.Per(pre))
}

func debug(ctx context.Context, n int, name string, view func() uint64) {
	if n <= 0 {
		n = 1
	}
	go func() {
		tk := time.NewTicker(time.Duration(n) * time.Second)
		defer tk.Stop()
		for {
			select {
			case <-ctx.Done():
				return

			case <-tk.C:
				xEnv.Errorf("%s elastic.consumer total %d", name, view())
			}
		}
	}()
}
