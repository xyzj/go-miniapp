package tcp

import (
	"fmt"
	"net"
	"slices"
	"time"

	"github.com/xyzj/gomicroapp/discovery"
	"github.com/xyzj/gomicroapp/framework"
	"github.com/xyzj/toolbox/tcpfactory"
)

type Config struct {
	PublicAddr        string `mapstructure:"public_addr" yaml:"public_addr"`                 // 对外服务地址，格式 "host:port"，如 "
	BindAddr          string `mapstructure:"bind_addr" yaml:"bind_addr"`                     // 监听地址，ip:port 格式，默认为 ":1109"
	Port              int    `mapstructure:"port" yaml:"port"`                               // 监听端口，默认为 1109
	KeepAliveInterval int    `mapstructure:"keep_alive_interval" yaml:"keep_alive_interval"` // TCP KeepAlive 间隔时间，单位秒，默认为 30 秒
	ReadTimeoutSec    int    `mapstructure:"read_timeout_sec" yaml:"read_timeout_sec"`       // 读超时时间，单位秒，默认为 100 秒
	WriteTimeoutSec   int    `mapstructure:"write_timeout_sec" yaml:"write_timeout_sec"`     // 写超时时间，单位秒，默认为 15 秒
}

type Module struct {
	cfg     Config
	name    string
	manager *tcpfactory.TCPManager
	client  tcpfactory.Client
	depend  []string
}
type ModuleOptions func(*Module)

func (m *Module) WithName(name string) *Module {
	m.name = name
	return m
}
func (m *Module) WithClient(client tcpfactory.Client) *Module {
	m.client = client
	return m
}

func New(opt ...ModuleOptions) *Module {
	m := &Module{
		cfg:    Config{},
		name:   "tcp",
		client: &tcpfactory.EchoClient{}, // 默认使用 EchoClient，用户可以通过 WithClient 方法替换为自定义的 Client 实现
	}
	for _, o := range opt {
		o(m)
	}
	return m
}

func (m *Module) Name() string { return m.name }

func (m *Module) Health() error {
	if m.manager == nil {
		return fmt.Errorf("tcp manager is not initialized")
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
		m.cfg.Port = 1109
		m.cfg.ReadTimeoutSec = 100
		m.cfg.WriteTimeoutSec = 15
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
				ctx.Logger().Error("[TCP] discovery module not found, skipping service registration")
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
					ctx.Logger().Error("[TCP] service registration failed: " + err.Error())
					return err
				}
			} else {
				ctx.Logger().Error("[TCP] discovery module does not implement Discovery interface, skipping service registration")
				return fmt.Errorf("discovery module does not implement Discovery interface")
			}
		}
		return nil
	}
	var err error
	m.manager, err = tcpfactory.NewTcpFactory(
		tcpfactory.WithBindAddr(net.JoinHostPort(m.cfg.BindAddr, fmt.Sprintf("%d", m.cfg.Port))),
		tcpfactory.WithKeepAlive(time.Duration(m.cfg.KeepAliveInterval)*time.Second),
		tcpfactory.WithReadTimeout(time.Duration(m.cfg.ReadTimeoutSec)*time.Second),
		tcpfactory.WithWriteTimeout(time.Duration(m.cfg.WriteTimeoutSec)*time.Second),
		tcpfactory.WithTcpClient(m.client),
		tcpfactory.WithLogger(ctx.Logger()),
	)
	if err != nil {
		return err
	}
	reg("tcp")
	return m.manager.Listen()
}

func (m *Module) Stop() error {
	if m.manager != nil {
		return m.manager.Shutdown()
	}
	m.manager = nil
	return nil
}

func (m *Module) DependsOn() []string {
	return m.depend
}
