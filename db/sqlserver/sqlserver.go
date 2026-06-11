package sqlserver

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/xyzj/miniapp/db"
	"github.com/xyzj/miniapp/framework"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
)

type Config struct {
	Addr                   string `mapstructure:"addr" yaml:"addr"`
	Port                   uint16 `mapstructure:"port" yaml:"port"`
	Username               string `mapstructure:"username" yaml:"username"`
	Password               string `mapstructure:"password" yaml:"password"`
	DBNames                string `mapstructure:"db_names" yaml:"db_names"`
	Encrypt                string `mapstructure:"encrypt" yaml:"encrypt"` // disable, false, true
	TrustServerCertificate bool   `mapstructure:"trust_server_certificate" yaml:"trust_server_certificate"`
	MaxOpenConns           int    `mapstructure:"max_open_conns" yaml:"max_open_conns"`
	MaxIdleConns           int    `mapstructure:"max_idle_conns" yaml:"max_idle_conns"`
	ConnMaxLifetime        int    `mapstructure:"conn_max_lifetime_sec" yaml:"conn_max_lifetime_sec"`
	PingTimeoutSec         int    `mapstructure:"ping_timeout_sec" yaml:"ping_timeout_sec"`
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
		name: "sqlserver",
		cfg:  Config{},
	}
	for _, o := range opt {
		o(m)
	}
	return m
}

func (m *Module) Name() string { return m.name }

func (m *Module) Health() error {
	if m.db == nil {
		return fmt.Errorf("sqlserver client is not initialized")
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
			Addr:                   "127.0.0.1",
			Port:                   1433,
			Username:               "sa",
			Password:               "YourStrong@Passw0rd",
			DBNames:                "master",
			Encrypt:                "disable",
			TrustServerCertificate: true,
			MaxOpenConns:           20,
			MaxIdleConns:           10,
			ConnMaxLifetime:        300,
			PingTimeoutSec:         3,
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
	defdb = strings.TrimSpace(defdb)
	if defdb == "" {
		return fmt.Errorf("default database name is empty")
	}

	q := url.Values{}
	if m.cfg.Encrypt != "" {
		q.Set("encrypt", m.cfg.Encrypt)
	}
	if m.cfg.TrustServerCertificate {
		q.Set("TrustServerCertificate", "true")
	}
	q.Set("database", defdb)

	dsn := (&url.URL{
		Scheme:   "sqlserver",
		User:     url.UserPassword(m.cfg.Username, framework.DeobfuscatePassword(m.cfg.Password)),
		Host:     net.JoinHostPort(m.cfg.Addr, strconv.Itoa(int(m.cfg.Port))),
		RawQuery: q.Encode(),
	}).String()

	orm, err := gorm.Open(sqlserver.Open(dsn))
	if err != nil {
		ctx.Logger().Error("[SQLSERVER] gorm open failed: " + err.Error())
		return err
	}

	sqldb, err := orm.DB()
	if err != nil {
		ctx.Logger().Error("[SQLSERVER] get sql.DB failed: " + err.Error())
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
		ctx.Logger().Error("[SQLSERVER] ping failed: " + err.Error())
		return err
	}
	cancel()
	m.db = db.NewDB(defdb, m.cfg.DBNames, "sqlserver", orm, sqldb)
	ctx.Provide(m.name, m.db)
	return nil
}

func (m *Module) Stop(ctx framework.Context) error {
	if m.db == nil {
		return nil
	}
	if err := m.db.SQL().Close(); err != nil {
		ctx.Logger().Error("[SQLSERVER] close failed: " + err.Error())
	}
	m.db = nil
	return nil
}

func (m *Module) Cli() *db.DB {
	return m.db
}
