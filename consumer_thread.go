package kfk

import (
	"context"
	"github.com/segmentio/kafka-go"
)

type handler func(kafka.Message)

type ConsumerThread struct {
	cfg *config
	ID  int
	ctx context.Context
	r   *kafka.Reader
	hd  handler
}

func NewConsumerThread(ctx context.Context, cfg *config, id int, hd handler) *ConsumerThread {
	cth := &ConsumerThread{
		cfg: cfg,
		ctx: ctx,
		ID:  id,
		hd:  hd,
	}

	cth.constructor()

	return cth
}

func (cth *ConsumerThread) constructor() {
	cth.r = kafka.NewReader(kafka.ReaderConfig{
		Brokers:     cth.cfg.addr,
		GroupID:     cth.cfg.GroupID(),
		GroupTopics: cth.cfg.topics,
	})
}

func (cth *ConsumerThread) Do(km kafka.Message) {
	if cth.hd == nil {
		return
	}

	cth.hd(km)
}

func (cth *ConsumerThread) Loop() {
	defer cth.r.Close()

	for {
		select {
		case <-cth.ctx.Done():
			xEnv.Infof("%s consumer.thread=%d exit", cth.cfg.name, cth.ID)
			return
		default:
			cth.cfg.wait()

			m, err := cth.r.ReadMessage(cth.ctx)
			if err != nil {
				xEnv.Errorf("%s consumer.thread=%d read fail %v", cth.cfg.name, cth.ID, err)
				continue
			}
			cth.Do(m)
		}
	}
}
