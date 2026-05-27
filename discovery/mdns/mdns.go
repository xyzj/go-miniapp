package mdns

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/grandcat/zeroconf"
	"github.com/xyzj/gomicroapp/discovery"
	"github.com/xyzj/gomicroapp/framework"
)

type Config struct {
	ServiceGroup string `mapstructure:"service_group" yaml:"service_group"` // 服务组名，用于区分一个网络中不同组的服务，如 "myapp-http"
}
type MDNSDiscovery struct {
	server    map[string]*zeroconf.Server
	resolver  *zeroconf.Resolver
	cfg       Config
	name      string
	infoCache *discovery.ServiceInfo
}

func New() *MDNSDiscovery {
	return &MDNSDiscovery{name: "discovery"}
}
func (m *MDNSDiscovery) Name() string { return m.name }

func (m *MDNSDiscovery) Health() error {
	if m.resolver == nil {
		return fmt.Errorf("mDNS resolver is not initialized")
	}
	return nil
}

func (m *MDNSDiscovery) Init(reg framework.Registry) error {
	if reg == nil {
		return fmt.Errorf("registry is nil")
	}
	if err := reg.UnmarshalKey(m.name, &m.cfg); err != nil {
		return err
	}
	if m.cfg.ServiceGroup == "" {
		m.cfg.ServiceGroup = "gomicroapp"
		if err := reg.MergeConfig(m.name, m.cfg); err != nil {
			return err
		}
	}
	m.server = make(map[string]*zeroconf.Server)
	m.infoCache = discovery.NewServiceInfo()
	m.cfg.ServiceGroup = "_" + m.cfg.ServiceGroup + "._tcp"
	return nil
}

func (m *MDNSDiscovery) Start(ctx framework.Context) error {
	// 发现局域网内的其他服务
	resolver, _ := zeroconf.NewResolver(nil)
	entries := make(chan *zeroconf.ServiceEntry)
	tc := time.NewTimer(discovery.DiscoveryInterval())
	go func() {
		for {
			select {
			case entry := <-entries:
				if len(entry.Text) < 3 {
					continue
				}
				ips := make([]string, 0, len(entry.AddrIPv4)+len(entry.AddrIPv6))
				for _, ip := range entry.AddrIPv4 {
					ips = append(ips, ip.String())
				}
				for _, ip := range entry.AddrIPv6 {
					ips = append(ips, ip.String())
				}
				// 缓存服务信息，供路由模块使用
				m.infoCache.Store(&discovery.Info{
					Name:       entry.Instance,
					LocalPort:  entry.Port,
					SourceIP:   ips,
					Version:    entry.Text[0][8:], // version=1.0.0
					Protocol:   entry.Text[1][9:], // protocol=http
					PublicAddr: entry.Text[2][7:], // public=[ip:port]
				})
			case <-ctx.Done():
				return
			}
		}
	}()
	for {
		select {
		case <-tc.C:
			ctxb, cancel := context.WithTimeout(ctx, time.Second)
			resolver.Browse(ctxb, m.cfg.ServiceGroup, "local.", entries)
			cancel()
			tc.Reset(discovery.DiscoveryInterval())
		case <-ctx.Done():
			return nil
		}
	}
}

func (m *MDNSDiscovery) Stop(ctx framework.Context) error {
	for _, server := range m.server {
		server.Shutdown()
	}
	m.server = nil
	m.resolver = nil
	return nil
}

func (m *MDNSDiscovery) Register(info *discovery.Info) error {
	pid := os.Getpid()
	instance := fmt.Sprintf("%s (%d)", info.Name, pid)
	server, err := zeroconf.Register(instance,
		m.cfg.ServiceGroup,
		"local.",
		info.LocalPort,
		[]string{
			"version=" + info.Version,
			"protocol=" + info.Protocol,
			"public=" + info.PublicAddr,
			"name=" + info.Name}, nil)
	if err != nil {
		return err
	}
	m.server[info.Name] = server
	return nil
}

func (m *MDNSDiscovery) Find(name string) (*discovery.Info, error) {
	// 从缓存中查找服务信息
	if info, found := m.infoCache.Load(name); found {
		return info, nil
	}
	return nil, fmt.Errorf("service not found: %s", name)
}
