package framework

import (
	"context"

	gocmd "github.com/xyzj/go-cmd"
	"github.com/xyzj/toolbox/logger"
)

func NewLoggerContext(ctx context.Context, log logger.Logger) *Context {
	return &Context{
		Context: ctx,
		options: &option{
			logger: log,
		},
	}
}

type Context struct {
	context.Context
	reg     Registry
	options *option
}

func (c *Context) Logger() logger.Logger {
	if c.options == nil || c.options.logger == nil {
		return logger.NewNilLogger()
	}
	return c.options.logger
}

func (c *Context) Debug() bool {
	if c.options == nil {
		return false
	}
	return c.options.debug
}

func (c *Context) AppInfo() gocmd.Info {
	if c.options == nil || c.options.version == nil {
		return gocmd.Info{}
	}
	return *c.options.version
}

func (c *Context) Provide(name string, instance any) {
	if c.reg == nil {
		return
	}
	c.reg.Provide(name, instance)
}

func (c *Context) Get(name string) (any, bool) {
	if c.reg == nil {
		return nil, false
	}
	return c.reg.Get(name)
}
