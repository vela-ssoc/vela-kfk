package kfk

import (
	"github.com/segmentio/kafka-go"
	"github.com/vela-ssoc/vela-kit/lua"
)

type Message struct {
	value kafka.Message
}

func (m Message) String() string                         { return "" }
func (m Message) Type() lua.LValueType                   { return lua.LTObject }
func (m Message) AssertFloat64() (float64, bool)         { return 0, false }
func (m Message) AssertString() (string, bool)           { return "", false }
func (m Message) AssertFunction() (*lua.LFunction, bool) { return nil, false }
func (m Message) Peek() lua.LValue                       { return m }

func (m Message) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "topic":
		return lua.S2L(m.value.Topic)
	case "value":
		return lua.B2L(m.value.Value)
	case "key":
		return lua.B2L(m.value.Key)
	case "partition":
		return lua.LInt(m.value.Partition)
	}

	return lua.LNil
}
