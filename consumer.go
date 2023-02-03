package kfk

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/vela-ssoc/vela-kit/lua"
	"reflect"
	"sync/atomic"
	"time"
)

var consumerTypeOf = reflect.TypeOf((*Consumer)(nil)).String()

type Consumer struct {
	lua.SuperVelaData
	cfg    *config
	total  uint64
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *Consumer) Name() string {
	return c.cfg.name
}

func (c *Consumer) Type() string {
	return consumerTypeOf
}

func (c *Consumer) call(v kafka.Message) {
	val := Message{v}
	if c.cfg.pipe != nil {
		c.cfg.pipe.Do(val, c.cfg.co, func(err error) {
			xEnv.Errorf("%s consumer pipe call fail %v", c.Name(), err)
		})
	}

	if c.cfg.vsh != nil {
		c.cfg.vsh.Do(val)
	}
}

func (c *Consumer) output(m kafka.Message) {
	if len(c.cfg.output) == 0 {
		return
	}

	for _, out := range c.cfg.output {
		out.Write(m.Value)
	}
}

func (c *Consumer) handle(v kafka.Message) {
	atomic.AddUint64(&c.total, 1)
	c.output(v)
	c.call(v)
}

func (c *Consumer) view() uint64 {
	return c.total
}

func (c *Consumer) run(n int) {
	for i := 0; i < n; i++ {
		cth := NewConsumerThread(c.ctx, c.cfg, i, c.handle)
		go cth.Loop()
	}
}

func (c *Consumer) Start() error {
	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	c.cancel = cancel

	if c.cfg.thread <= 0 {
		c.run(3)
	} else {
		c.run(c.cfg.thread)
	}

	return nil
}

func (c *Consumer) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	return nil
}

func NewConsumer(cfg *config) *Consumer {
	c := &Consumer{cfg: cfg}
	c.V(lua.VTInit, time.Now())
	return c
}
