package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/xyzj/gomicroapp/framework"
)

type Config struct {
	Addr            string `mapstructure:"addr" yaml:"addr"`
	Username        string `mapstructure:"username" yaml:"username"`
	Password        string `mapstructure:"password" yaml:"password"`
	DB              int    `mapstructure:"db" yaml:"db"`
	DialTimeoutSec  int    `mapstructure:"dial_timeout_sec" yaml:"dial_timeout_sec"`
	ReadTimeoutSec  int    `mapstructure:"read_timeout_sec" yaml:"read_timeout_sec"`
	WriteTimeoutSec int    `mapstructure:"write_timeout_sec" yaml:"write_timeout_sec"`
	PoolSize        int    `mapstructure:"pool_size" yaml:"pool_size"`
	MinIdleConns    int    `mapstructure:"min_idle_conns" yaml:"min_idle_conns"`
	PingTimeoutSec  int    `mapstructure:"ping_timeout_sec" yaml:"ping_timeout_sec"`
}

type Module struct {
	cfg  Config
	cli  *redis.Client
	name string
}
type ModuleOptions func(*Module)

func (m *Module) WithName(name string) *Module {
	m.name = name
	return m
}

func New(opt ...ModuleOptions) *Module {
	m := &Module{
		cfg:  Config{},
		name: "redis",
	}
	for _, o := range opt {
		o(m)
	}
	return m
}

func (m *Module) Name() string { return m.name }

func (m *Module) Health() error {
	if m.cli == nil {
		return fmt.Errorf("redis client is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.PingTimeoutSec)*time.Second)
	defer cancel()
	return m.cli.Ping(ctx).Err()
}

func (m *Module) Init(reg framework.Registry) error {
	if reg == nil {
		return fmt.Errorf("registry is nil")
	}
	if err := reg.UnmarshalKey(m.name, &m.cfg); err != nil {
		return err
	}
	if m.cfg.Addr == "" {
		m.cfg = Config{
			Addr:            "127.0.0.1:6379",
			Username:        "",
			Password:        "",
			DB:              0,
			DialTimeoutSec:  3,
			ReadTimeoutSec:  3,
			WriteTimeoutSec: 3,
			PoolSize:        20,
			MinIdleConns:    2,
			PingTimeoutSec:  3,
		}
		if err := reg.MergeConfig(m.name, m.cfg); err != nil {
			return err
		}
	}
	if m.cfg.PingTimeoutSec <= 0 {
		m.cfg.PingTimeoutSec = 3
	}
	return nil
}

func (m *Module) Start(ctx framework.Context) error {
	opts := &redis.Options{
		Addr:         m.cfg.Addr,
		Username:     m.cfg.Username,
		Password:     framework.DeobfuscatePassword(m.cfg.Password),
		DB:           m.cfg.DB,
		DialTimeout:  time.Duration(m.cfg.DialTimeoutSec) * time.Second,
		ReadTimeout:  time.Duration(m.cfg.ReadTimeoutSec) * time.Second,
		WriteTimeout: time.Duration(m.cfg.WriteTimeoutSec) * time.Second,
		PoolSize:     m.cfg.PoolSize,
		MinIdleConns: m.cfg.MinIdleConns,
	}
	cli := redis.NewClient(opts)
	pingCtx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.PingTimeoutSec)*time.Second)
	defer cancel()
	if err := cli.Ping(pingCtx).Err(); err != nil {
		_ = cli.Close()
		ctx.Logger().Error("[REDIS] ping failed: " + err.Error())
		return err
	}
	m.cli = cli
	ctx.Provide(m.name, m.cli)
	return nil
}

func (m *Module) Stop(ctx framework.Context) error {
	if m.cli == nil {
		return nil
	}
	if err := m.cli.Close(); err != nil {
		ctx.Logger().Error("[REDIS] close failed: " + err.Error())
		return err
	}
	m.cli = nil
	return nil
}

func (m *Module) Cli() *redis.Client {
	return m.cli
}
