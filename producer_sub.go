package kfk

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/vela-ssoc/vela-kit/lua"
	"reflect"
)

var producerSubTypeOf = reflect.TypeOf((*producerSub)(nil)).String()

type producerSub struct {
	lua.SuperVelaData
	parent *Producer
	topic  string
}

func (sub *producerSub) Name() string {
	return fmt.Sprintf("%s.%s", sub.parent.Name(), sub.topic)
}

func (sub *producerSub) Type() string {
	return producerSubTypeOf
}

func (sub *producerSub) Start() error {
	return nil
}

func (sub *producerSub) Close() error {
	return nil
}

func (sub *producerSub) Write(v []byte) (n int, err error) {
	if len(v) == 0 {
		return
	}
	msg := kafka.Message{
		Topic: sub.topic,
		Value: v,
	}

	err = sub.parent.push(msg)
	if err != nil {
		return 0, err
	}

	return len(v), nil
}

func (sub *producerSub) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "push":
		return lua.NewFunction(sub.parent.pushL)
	}

	return lua.LNil
}
