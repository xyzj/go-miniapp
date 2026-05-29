package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/xyzj/gomicroapp/db"
	"github.com/xyzj/gomicroapp/framework"
	_ "modernc.org/sqlite"
)

type Config struct {
	DSN             string `mapstructure:"dsn" yaml:"dsn"` // 数据库连接字符串，支持相对路径和绝对路径
	MaxOpenConns    int    `mapstructure:"max_open_conns" yaml:"max_open_conns"`
	MaxIdleConns    int    `mapstructure:"max_idle_conns" yaml:"max_idle_conns"`
	ConnMaxLifetime int    `mapstructure:"conn_max_lifetime_sec" yaml:"conn_max_lifetime_sec"`
	PingTimeoutSec  int    `mapstructure:"ping_timeout_sec" yaml:"ping_timeout_sec"`
}

type Module struct {
	cfg  Config
	db   *db.DB
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
		name: "sqlite",
	}
	for _, o := range opt {
		o(m)
	}
	return m
}

func (m *Module) Name() string { return m.name }

func (m *Module) Health() error {
	if m.db == nil {
		return fmt.Errorf("sqlite client is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.PingTimeoutSec)*time.Second)
	defer cancel()
	return m.db.SQL().PingContext(ctx)
}

func (m *Module) Init(reg framework.Registry) error {
	if reg == nil {
		return fmt.Errorf("registry is nil")
	}
	if err := reg.UnmarshalKey(m.name, &m.cfg); err != nil {
		return err
	}
	if m.cfg.DSN == "" {
		m.cfg = Config{
			DSN:             "./data/app.db",
			MaxOpenConns:    1,
			MaxIdleConns:    1,
			ConnMaxLifetime: 300,
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
	dir := filepath.Dir(m.cfg.DSN)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	sqldb, err := sql.Open("sqlite", m.cfg.DSN)
	if err != nil {
		ctx.Logger().Error("[SQLITE] open failed: " + err.Error())
		return err
	}
	if m.cfg.MaxOpenConns > 0 {
		sqldb.SetMaxOpenConns(m.cfg.MaxOpenConns)
	}
	if m.cfg.MaxIdleConns > 0 {
		sqldb.SetMaxIdleConns(m.cfg.MaxIdleConns)
	}
	if m.cfg.ConnMaxLifetime > 0 {
		sqldb.SetConnMaxLifetime(time.Duration(m.cfg.ConnMaxLifetime) * time.Second)
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), time.Duration(m.cfg.PingTimeoutSec)*time.Second)
	defer cancel()
	if err := sqldb.PingContext(pingCtx); err != nil {
		_ = sqldb.Close()
		ctx.Logger().Error("[SQLITE] ping failed: " + err.Error())
		return err
	}
	m.db = db.NewDB("", "", "sqlite", nil, sqldb)
	ctx.Provide(m.name, m.db)
	return nil
}

func (m *Module) Stop(ctx framework.Context) error {
	if m.db == nil {
		return nil
	}
	if err := m.db.SQL().Close(); err != nil {
		ctx.Logger().Error("[SQLITE] close failed: " + err.Error())
		return err
	}
	m.db = nil
	return nil
}

func (m *Module) Cli() *db.DB {
	return m.db
}
