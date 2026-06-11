package http

import (
	"fmt"
	"net"
	"net/http"
	"slices"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/xyzj/gominiapp/discovery"
	"github.com/xyzj/gominiapp/framework"
	"github.com/xyzj/toolbox/crypto"
)

type Config struct {
	PublicAddr        string `mapstructure:"public_addr" yaml:"public_addr"` // 对外服务地址，格式 "host:port"，如 "
	BindAddr          string `mapstructure:"bind_addr" yaml:"bind_addr"`
	CertFile          string `mapstructure:"cert_file" yaml:"cert_file"`
	KeyFile           string `mapstructure:"key_file" yaml:"key_file"`
	Port              int    `mapstructure:"port" yaml:"port"`
	ReadTimeout       int    `mapstructure:"read_timeout" yaml:"read_timeout"`
	WriteTimeout      int    `mapstructure:"write_timeout" yaml:"write_timeout"`
	ReadHeaderTimeout int    `mapstructure:"read_header_timeout" yaml:"read_header_timeout"`
}

type Module struct {
	name   string
	cfg    Config
	opt    *option
	s      *http.Server
	depend []string
}

func New(name string, opts ...Options) *Module {
	if name == "" {
		name = "http"
	}
	module := &Module{
		name: name,
		opt: &option{
			cors:    cors.DefaultConfig(),
			handler: nil,
		},
	}
	for _, opt := range opts {
		opt(module.opt)
	}
	return module
}

func (m *Module) Name() string { return m.name }

func (m *Module) Health() error {
	if m.s == nil {
		return fmt.Errorf("http server is not initialized")
	}
	if m.opt.handler == nil {
		return fmt.Errorf("http engine is not initialized")
	}
	return nil
}

func (m *Module) Init(reg framework.Registry) error {
	if reg == nil {
		return fmt.Errorf("registry is nil")
	}
	if err := reg.UnmarshalKey(m.name, &m.cfg); err != nil {
		return err
	}
	if m.cfg.Port == 0 {
		m.cfg.Port = 6819
		m.cfg.ReadTimeout = 100
		m.cfg.WriteTimeout = 15
		m.cfg.ReadHeaderTimeout = 2
		if err := reg.MergeConfig(m.name, m.cfg); err != nil {
			return err
		}
	}

	if m.cfg.PublicAddr != "" {
		_, _, err := net.SplitHostPort(m.cfg.PublicAddr)
		if err == nil {
			m.depend = append(m.depend, "discovery")
		} else {
			n := net.ParseIP(m.cfg.PublicAddr)
			if n != nil {
				m.cfg.PublicAddr = net.JoinHostPort(m.cfg.PublicAddr, fmt.Sprintf("%d", m.cfg.Port))
				m.depend = append(m.depend, "discovery")
			}
		}
	}
	return nil
}

func (m *Module) Start(ctx framework.Context) error {
	reg := func(protocol string) error {
		if slices.Contains(m.depend, "discovery") {
			dis, ok := ctx.Get("discovery")
			if !ok {
				ctx.Logger().Error("[HTTP] discovery module not found, skipping service registration")
				return fmt.Errorf("discovery module not found")
			}
			if di, ok := dis.(discovery.Discovery); ok {
				if err := di.Register(&discovery.Info{
					Name:       ctx.AppInfo().Name,
					Protocol:   protocol,
					Version:    ctx.AppInfo().Version,
					PublicAddr: m.cfg.PublicAddr,
					LocalPort:  m.cfg.Port,
				}); err != nil {
					ctx.Logger().Error("[HTTP] service registration failed: " + err.Error())
					return err
				}
			} else {
				ctx.Logger().Error("[HTTP] discovery module does not implement Discovery interface, skipping service registration")
				return fmt.Errorf("discovery module does not implement Discovery interface")
			}
		}
		return nil
	}
	m.s = &http.Server{
		Addr:              net.JoinHostPort(m.cfg.BindAddr, fmt.Sprintf("%d", m.cfg.Port)),
		Handler:           m.opt.handler,
		ReadTimeout:       time.Duration(m.cfg.ReadTimeout) * time.Second,
		WriteTimeout:      time.Duration(m.cfg.WriteTimeout) * time.Second,
		IdleTimeout:       time.Duration(m.cfg.WriteTimeout) * time.Second,
		ReadHeaderTimeout: time.Duration(m.cfg.ReadHeaderTimeout) * time.Second,
	}
	tc, err := crypto.TLSConfigFromFile(m.cfg.CertFile, m.cfg.KeyFile, "")
	if err == nil {
		m.s.TLSConfig = tc
		reg("https")
		return m.s.ListenAndServeTLS("", "")
	}
	reg("http")
	return m.s.ListenAndServe()
}

func (m *Module) Stop(ctx framework.Context) error {
	if m.s == nil {
		return nil
	}
	return m.s.Shutdown(ctx)
}

func (m *Module) DependsOn() []string {
	return m.depend
}
