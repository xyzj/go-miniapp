package bolt

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/xyzj/gomicroapp/framework"
	"github.com/xyzj/toolbox/db"
)

type Config struct {
	Path       string `mapstructure:"path" yaml:"path"`               // BoltDB 文件路径，支持相对路径和绝对路径
	TimeoutSec int    `mapstructure:"timeout_sec" yaml:"timeout_sec"` // BoltDB 打开超时时间，单位秒
}

type Module struct {
	cfg  Config
	db   *db.BoltDB
	name string
}

func New(name string) *Module {
	if name == "" {
		name = "bolt"
	}
	return &Module{
		name: name,
		db:   nil,
	}
}

func (m *Module) Name() string { return m.name }

func (m *Module) Health() error {
	if m.db == nil {
		return fmt.Errorf("bolt client is not initialized")
	}
	return m.db.Health()
}

func (m *Module) Init(reg framework.Registry) error {
	if reg == nil {
		return fmt.Errorf("registry is nil")
	}
	if err := reg.UnmarshalKey(m.name, &m.cfg); err != nil {
		return err
	}
	if m.cfg.Path == "" {
		m.cfg = Config{
			Path:       "./app.db",
			TimeoutSec: 3,
		}
		if err := reg.MergeConfig(m.name, m.cfg); err != nil {
			return err
		}
	}
	if m.cfg.TimeoutSec <= 0 {
		m.cfg.TimeoutSec = 3
	}
	return nil
}

func (m *Module) Start(ctx framework.Context) error {
	dir := filepath.Dir(m.cfg.Path)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	db, err := db.NewBolt(m.cfg.Path)
	if err != nil {
		ctx.Logger().Error("[BOLT] open failed: " + err.Error())
		return err
	}
	m.db = db
	ctx.Provide(m.name, m.db)
	return nil
}

func (m *Module) Stop(ctx framework.Context) error {
	if m.db == nil {
		return nil
	}
	if err := m.db.Close(); err != nil {
		ctx.Logger().Error("[BOLT] close failed: " + err.Error())
		return err
	}
	m.db = nil
	return nil
}

func (m *Module) Cli() *db.BoltDB {
	return m.db
}
