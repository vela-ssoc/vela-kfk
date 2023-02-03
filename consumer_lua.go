package kfk

import (
	"github.com/vela-ssoc/vela-kit/lua"
	"github.com/vela-ssoc/vela-kit/pipe"
	vswitch "github.com/vela-ssoc/vela-switch"
)

func (c *Consumer) limitL(L *lua.LState) int {
	n := L.IsInt(1)
	pre := L.IsInt(2)
	c.cfg.limit = newLimiter(n, pre)
	return 0
}

func (c *Consumer) startL(L *lua.LState) int {
	xEnv.Start(L, c).From(L.CodeVM()).Do()
	return 0
}

func (c *Consumer) selectL(L *lua.LState) int {
	vsh := vswitch.CheckSwitch(L, 1)
	c.cfg.vsh = vsh
	return 0
}

func (c *Consumer) pipeL(L *lua.LState) int {
	c.cfg.pipe = pipe.NewByLua(L, pipe.Env(xEnv))
	return 0
}

func (c *Consumer) toL(L *lua.LState) int {
	n := L.GetTop()
	if n == 0 {
		return 0
	}

	for i := 1; i <= n; i++ {
		data := L.CheckVelaData(i)
		c.cfg.output = append(c.cfg.output, lua.CheckWriter(data))
	}
	return 0
}

func (c *Consumer) debugL(L *lua.LState) int {
	n := L.CheckInt(1)
	debug(c.ctx, n, c.Name(), c.view)
	return 0
}

func (c *Consumer) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "start":
		return lua.NewFunction(c.startL)
	case "select":
		return lua.NewFunction(c.selectL)
	case "pipe":
		return lua.NewFunction(c.pipeL)

	case "to":
		return lua.NewFunction(c.toL)
	case "debug":
		return lua.NewFunction(c.debugL)
	case "limit":
		return lua.NewFunction(c.limitL)

	}

	return lua.LNil
}
