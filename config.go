package kfk

import (
	"fmt"
	auxlib2 "github.com/vela-ssoc/vela-kit/auxlib"
	"github.com/vela-ssoc/vela-kit/lua"
	"github.com/vela-ssoc/vela-kit/pipe"
	vswitch "github.com/vela-ssoc/vela-switch"
	"go.uber.org/ratelimit"
)

type config struct {
	proxy       bool
	name        string            //consumer, producer name
	compression string            //producer compression
	balancer    string            //producer balancer
	ack         string            //producer RequireAck
	async       bool              //producer async
	timeout     int               //producer , consumer timeout
	limit       ratelimit.Limiter //producer , consumer limit
	addr        []string          //producer , consumer broker addr
	topics      []string          //consumer: group topics , producer: topics[0]
	groupID     string            //consumer gorup id
	thread      int               //consumer thread
	pipe        *pipe.Chains
	vsh         *vswitch.Switch
	output      []lua.Writer
	co          *lua.LState
}

func newConfig(L *lua.LState) *config {
	tab := L.CheckTable(1)

	cfg := &config{
		co:    xEnv.Clone(L),
		async: true,
	}

	tab.Range(func(key string, val lua.LValue) {
		cfg.NewIndex(L, key, val)
	})

	if err := cfg.valid(); err != nil {
		L.RaiseError("kfk client config fail %v", err)
		return nil
	}

	return cfg
}

func (cfg *config) NewIndex(L *lua.LState, key string, val lua.LValue) {
	switch key {
	case "name":
		cfg.name = lua.CheckString(L, val)
	case "group_id":
		cfg.groupID = lua.CheckString(L, val)
	case "compression":
		cfg.compression = lua.CheckString(L, val)
	case "balancer":
		cfg.balancer = lua.CheckString(L, val)
	case "async":
		cfg.async = lua.IsTrue(val)
	case "thread":
		cfg.thread = lua.CheckInt(L, val)
	case "ack":
		cfg.ack = val.String()
	case "timeout":
		cfg.timeout = lua.CheckInt(L, val)
	case "proxy":
		cfg.proxy = lua.CheckBool(L, val)
	case "topic":
		switch val.Type() {
		case lua.LTString:
			cfg.topics = []string{val.String()}
		case lua.LTTable:
			cfg.topics = auxlib2.LTab2SS(val.(*lua.LTable))
		default:
			L.RaiseError("invalid topic , got %s", val.Type().String())

		}

	case "addr":
		switch val.Type() {
		case lua.LTString:
			cfg.addr = []string{val.String()}
		case lua.LTTable:
			cfg.addr = auxlib2.LTab2SS(val.(*lua.LTable))
		default:
			L.RaiseError("invalid addr , got %s", val.Type().String())
		}

	}
}

func (cfg *config) valid() error {
	if e := auxlib2.Name(cfg.name); e != nil {
		return e
	}

	if len(cfg.addr) == 0 {
		return fmt.Errorf("not found addr")
	}

	if len(cfg.topics) == 0 {
		return fmt.Errorf("not found topic")
	}

	for _, topic := range cfg.topics {
		if len(topic) == 0 {
			return fmt.Errorf("invalid topic value")
		}
	}
	return nil
}

func (cfg *config) GroupID() string {
	if cfg.groupID == "" {
		return "vela"
	}

	return cfg.groupID
}

func (cfg *config) wait() {
	if cfg.limit == nil {
		return
	}
	cfg.limit.Take()
}
