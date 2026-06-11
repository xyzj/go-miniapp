package pgsql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	dsn "github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/xyzj/miniapp/db"
	"github.com/xyzj/miniapp/framework"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Config struct {
	SSLMode         string `mapstructure:"sslmode" yaml:"sslmode"` // disable, allow, prefer, require, verify-ca, verify-full
	SSLRootCert     string `mapstructure:"sslrootcert" yaml:"sslrootcert"`
	Addr            string `mapstructure:"addr" yaml:"addr"`
	Port            uint16 `mapstructure:"port" yaml:"port"`
	Username        string `mapstructure:"username" yaml:"username"`
	Password        string `mapstructure:"password" yaml:"password"`
	DBNames         string `mapstructure:"db_names" yaml:"db_names"`
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
		name: "pgsql",
	}
	for _, o := range opt {
		o(m)
	}
	return m
}

func (m *Module) Name() string { return m.name }

func (m *Module) Health() error {
	if m.db == nil {
		return fmt.Errorf("pgsql client is not initialized")
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
	if m.cfg.Addr == "" {
		m.cfg = Config{
			Addr:            "127.0.0.1",
			Port:            5432,
			Username:        "postgres",
			Password:        "postgres",
			DBNames:         "app",
			SSLMode:         "prefer",
			SSLRootCert:     "",
			MaxOpenConns:    20,
			MaxIdleConns:    10,
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
	defdb := m.cfg.DBNames
	if strings.Contains(m.cfg.DBNames, ",") {
		defdb = strings.Split(m.cfg.DBNames, ",")[0]
	}
	sqlcfg := &pgx.ConnConfig{
		Config: dsn.Config{
			Host:     m.cfg.Addr,
			Port:     m.cfg.Port,
			User:     m.cfg.Username,
			Password: framework.DeobfuscatePassword(m.cfg.Password),
			Database: defdb,
			RuntimeParams: map[string]string{
				"sslmode":     m.cfg.SSLMode,
				"sslrootcert": m.cfg.SSLRootCert,
			},
		},
	}
	orm, err := gorm.Open(postgres.Open(stdlib.RegisterConnConfig(sqlcfg)))
	if err != nil {
		ctx.Logger().Error("[PGSQL] gorm open failed: " + err.Error())
		return err
	}
	sqldb, err := orm.DB()
	if err != nil {
		ctx.Logger().Error("[PGSQL] get sql.DB failed: " + err.Error())
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
	if err := sqldb.PingContext(pingCtx); err != nil {
		cancel()
		_ = sqldb.Close()
		ctx.Logger().Error("[PGSQL] ping failed: " + err.Error())
		return err
	}
	cancel()
	m.db = db.NewDB(defdb, m.cfg.DBNames, "postgresql", orm, sqldb)
	ctx.Provide(m.name, m.db)
	return nil
}

func (m *Module) Stop(ctx framework.Context) error {
	if m.db == nil {
		return nil
	}
	if err := m.db.SQL().Close(); err != nil {
		ctx.Logger().Error("[PGSQL] close failed: " + err.Error())
	}
	m.db = nil
	return nil
}

func (m *Module) Cli() *db.DB {
	return m.db
}
