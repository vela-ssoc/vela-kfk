package kfk

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/vela-ssoc/vela-kit/lua"
	"github.com/vela-ssoc/vela-kit/proxy"
	"net"
	"reflect"
	"sync/atomic"
	"time"
)

var producerTypeOf = reflect.TypeOf((*Producer)(nil)).String()

type Producer struct {
	lua.SuperVelaData
	cfg    *config
	total  uint64
	ctx    context.Context
	cancel context.CancelFunc
	w      *kafka.Writer
}

func (p *Producer) Name() string {
	return p.cfg.name
}

func (p *Producer) Type() string {
	return producerTypeOf
}

func (p *Producer) withCompression() {
	switch p.cfg.compression {
	case "gzip":
		p.w.Compression = kafka.Gzip
	case "snappy":
		p.w.Compression = kafka.Snappy
	case "lz4":
		p.w.Compression = kafka.Lz4
	case "zstd":
		p.w.Compression = kafka.Zstd
	default:
		//todo
	}
}

func (p *Producer) withBalancer() {
	switch p.cfg.balancer {
	case "least":
		p.w.Balancer = &kafka.LeastBytes{}
	case "crc32":
		p.w.Balancer = &kafka.CRC32Balancer{}
	case "hash":
		p.w.Balancer = &kafka.Hash{}
	case "rr":
		p.w.Balancer = &kafka.RoundRobin{}
	default:
		//todo
	}

}

func (p *Producer) withAsync() {
	if p.cfg.async {
		p.w.Async = true
	}
}

func (p *Producer) withTimeout() {
	if p.cfg.timeout <= 0 {
		return
	}
	p.w.WriteTimeout = time.Duration(p.cfg.timeout) * time.Second
}

func (p *Producer) withAck() {
	switch p.cfg.ack {
	case "one":
		p.w.RequiredAcks = kafka.RequireOne
	case "all":
		p.w.RequiredAcks = kafka.RequireAll
	default:
		p.w.RequiredAcks = kafka.RequireNone
	}
}

func (p *Producer) ProxyTransport() *kafka.Transport {
	return &kafka.Transport{
		Dial: func(ctx context.Context, network string, addr string) (net.Conn, error) {
			pxy := proxy.New(fmt.Sprintf("%s://%s", network, addr))
			return pxy.Dail(ctx)
		},
		DialTimeout: 30 * time.Second,
	}
}

func (p *Producer) Start() error {
	ctx, fn := context.WithCancel(context.Background())
	p.ctx = ctx
	p.cancel = fn

	p.w = &kafka.Writer{
		Addr:     kafka.TCP(p.cfg.addr...),
		Balancer: &kafka.LeastBytes{},
	}

	if p.cfg.proxy {
		p.w.Transport = p.ProxyTransport()
	}

	p.withAck()
	p.withTimeout()
	p.withAsync()
	p.withCompression()
	p.withBalancer()

	return nil
}

func (p *Producer) Close() error {
	if p.w == nil {
		return nil
	}
	p.cancel()

	return p.w.Close()
}

func (p *Producer) view() uint64 {
	return p.total
}

func (p *Producer) Write(v []byte) (n int, err error) {

	if len(v) == 0 {
		return
	}
	msg := kafka.Message{
		Topic: p.cfg.topics[0],
		Value: v,
	}

	err = p.push(msg)
	if err != nil {
		return 0, err
	}

	return len(v), nil
}

func (p *Producer) push(v ...kafka.Message) error {
	if p.w == nil {
		return fmt.Errorf("kafka producer %s not found valid writer", p.Name())
	}

	n := len(v)
	if n == 0 {
		return nil
	}

	atomic.AddUint64(&p.total, uint64(n))
	p.cfg.wait()
	return p.w.WriteMessages(p.ctx, v...)
}

func (p *Producer) Clone(topic string) *producerSub {
	sdk := &producerSub{topic: topic}
	sdk.parent = p
	sdk.topic = topic
	return sdk
}

func NewProducer(cfg *config) *Producer {
	p := &Producer{cfg: cfg}
	p.V(lua.VTInit, time.Now())
	return p
}
