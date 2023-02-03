package kfk

import (
	"github.com/segmentio/kafka-go"
	"github.com/vela-ssoc/vela-kit/lua"
)

func (p *Producer) startL(L *lua.LState) int {
	xEnv.Start(L, p).From(L.CodeVM()).Do()
	return 0
}

func (p *Producer) limitL(L *lua.LState) int {
	n := L.IsInt(1)
	pre := L.IsInt(2)

	p.cfg.limit = newLimiter(n, pre)
	return 0
}

func (p *Producer) pushL(L *lua.LState) int {
	n := L.GetTop()
	if n < 2 {
		return 0
	}

	topic := L.CheckString(1)
	msg := make([]kafka.Message, 0, n-1)

	for i := 2; i <= n; i++ {
		v := L.Get(i)
		msg = append(msg, kafka.Message{
			Topic: topic,
			Value: lua.S2B(v.String()),
		})
	}

	err := p.push(msg...)
	if err != nil {
		L.Push(lua.S2L(err.Error()))
		return 1
	}

	return 0
}

func (p *Producer) cloneL(L *lua.LState) int {
	topic := L.CheckString(1)
	vla := lua.NewVelaData(p.Clone(topic))
	L.Push(vla)
	return 1
}

func (p *Producer) debugL(L *lua.LState) int {
	n := L.CheckInt(1)
	debug(p.ctx, n, p.Name(), p.view)
	return 0
}

func (p *Producer) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "limit":
		return lua.NewFunction(p.limitL)
	case "start":
		return lua.NewFunction(p.startL)
	case "push":
		return lua.NewFunction(p.pushL)
	case "clone":
		return lua.NewFunction(p.cloneL)
	case "debug":
		return lua.NewFunction(p.debugL)
	}

	return lua.LNil
}
