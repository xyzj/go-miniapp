package mysql

import (
	"context"
	"fmt"
	"strings"
	"time"

	dsn "github.com/go-sql-driver/mysql"
	"github.com/xyzj/miniapp/db"
	"github.com/xyzj/miniapp/framework"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type Config struct {
	TLSConfig       string `mapstructure:"tls_config" yaml:"tls_config"` // false, skip-verify, custom
	Addr            string `mapstructure:"addr" yaml:"addr"`
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

type Options func(*Module)

func WithName(name string) Options {
	return func(m *Module) {
		m.name = name
	}
}

func New(opt ...Options) *Module {
	m := &Module{
		cfg:  Config{},
		name: "mysql",
	}
	for _, o := range opt {
		o(m)
	}
	return m
}

func (m *Module) Name() string { return m.name }

func (m *Module) Health() error {
	if m.db == nil {
		return fmt.Errorf("mysql client is not initialized")
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
			TLSConfig:       "false",
			Addr:            "127.0.0.1:3306",
			Username:        "root",
			Password:        "root",
			DBNames:         "app",
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
	sqlcfg := &dsn.Config{
		Collation:            "utf8mb4_general_ci",
		Loc:                  time.Local,
		User:                 m.cfg.Username,
		Passwd:               framework.DeobfuscatePassword(m.cfg.Password),
		Net:                  "tcp",
		Addr:                 m.cfg.Addr,
		DBName:               defdb,
		AllowNativePasswords: true,
		ParseTime:            true,
		MultiStatements:      true,
		Timeout:              time.Duration(m.cfg.ConnMaxLifetime) * time.Second,
		ClientFoundRows:      true,
		InterpolateParams:    true,
		TLSConfig:            m.cfg.TLSConfig,
	}
	orm, err := gorm.Open(mysql.Open(sqlcfg.FormatDSN()))
	if err != nil {
		ctx.Logger().Error("[MYSQL] gorm open failed: " + err.Error())
		return err
	}
	sqldb, err := orm.DB()
	if err != nil {
		ctx.Logger().Error("[MYSQL] get sql.DB failed: " + err.Error())
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
	var name, value, dbtype string
	err = sqldb.QueryRow("show variables like 'version_comment';").Scan(&name, &value)
	if err != nil {
		ctx.Logger().Error("[MYSQL] query version_comment failed: " + err.Error())
		return err
	} else {
		switch {
		case strings.Contains(strings.ToLower(value), "mariadb"):
			dbtype = "mariadb"
		case strings.Contains(strings.ToLower(value), "greatsql"):
			dbtype = "greatsql"
		default:
			dbtype = "mysql"
		}
	}
	m.db = db.NewDB(defdb, m.cfg.DBNames, dbtype, orm, sqldb)
	ctx.Provide(m.name, m.db)
	return nil
}

func (m *Module) Stop(ctx framework.Context) error {
	if m.db == nil {
		return nil
	}
	if err := m.db.SQL().Close(); err != nil {
		ctx.Logger().Error("[MYSQL] close failed: " + err.Error())
	}
	m.db = nil
	return nil
}

func (m *Module) Cli() *db.DB {
	return m.db
}
