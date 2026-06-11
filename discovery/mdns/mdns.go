package mdns

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/grandcat/zeroconf"
	"github.com/xyzj/gominiapp/discovery"
	"github.com/xyzj/gominiapp/framework"
	"github.com/xyzj/toolbox/cache"
)

const (
	lookupTimeout = 500 * time.Millisecond
	staleAfter    = 5 * time.Minute
	purgeAfter    = 8 * time.Minute // 超过指定时间彻底失联则强制从内存路由表中剔除
)

type MDNSDiscovery struct {
	server      map[string]*zeroconf.Server
	resolver    *zeroconf.Resolver
	cfg         discovery.Config
	name        string
	infoCache   *discovery.ServiceInfo
	lookupMap   sync.Map
	lookupCh    chan string
	removeCache *cache.AnyCache[string]
}

func New() *MDNSDiscovery {
	return &MDNSDiscovery{name: discovery.DefaultModuleName, cfg: discovery.Config{}}
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
		m.cfg.ServiceGroup = discovery.DefaultServiceGroup
		if err := reg.MergeConfig(m.name, m.cfg); err != nil {
			return err
		}
	}
	m.server = make(map[string]*zeroconf.Server)
	m.infoCache = discovery.NewServiceInfo(0)
	m.cfg.ServiceGroup = "_" + m.cfg.ServiceGroup + "._tcp"
	m.lookupMap = sync.Map{}
	m.lookupCh = make(chan string, 100)
	m.removeCache = cache.NewAnyCacheWithExpireFunc(time.Second*5, func(e map[string]string) {
		for key := range e {
			m.infoCache.Delete(key)
		}
	})
	return nil
}

func (m *MDNSDiscovery) Start(ctx framework.Context) error {
	// 发现局域网内的其他服务
	var err error
	m.resolver, err = zeroconf.NewResolver(nil)
	if err != nil {
		return err
	}
	entries := make(chan *zeroconf.ServiceEntry, 128)

	// Single consumer updates cache from both browse and lookup results.
	go func() {
		for {
			select {
			case entry, ok := <-entries:
				if !ok {
					return
				}
				if entry == nil {
					continue
				}
				func(entry *zeroconf.ServiceEntry) {
					defer func() {
						if r := recover(); r != nil {
							ctx.Logger().Error("panic in processing mDNS entry: " + r.(error).Error())
						}
					}()
					if entry.Service != m.cfg.ServiceGroup {
						return
					}
					if len(entry.Text) < 4 {
						return
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
						Instance:   entry.Instance,
						LocalPort:  entry.Port,
						SourceIP:   ips,
						Version:    strings.TrimPrefix(entry.Text[0], "version="),  // version=1.0.0
						Protocol:   strings.TrimPrefix(entry.Text[1], "protocol="), // protocol=http
						PublicAddr: strings.TrimPrefix(entry.Text[2], "public="),   // public=[ip:port]
						Name:       strings.TrimPrefix(entry.Text[3], "name="),     // name=serviceName
					})
					m.lookupMap.LoadAndDelete(entry.Instance)
				}(entry)
			case <-ctx.Done():
				close(entries)
				return
			}
		}
	}()

	// Dedicated worker performs stale-instance lookup requests.
	go func() {
		for {
			select {
			case instance, ok := <-m.lookupCh:
				if !ok {
					return
				}
				func(instance string) {
					// Always clear in-flight marker, even on timeout/errors.
					if _, loaded := m.lookupMap.Load(instance); !loaded {
						return
					}
					defer m.lookupMap.Delete(instance)
					lookupCtx, cancel := context.WithTimeout(ctx, lookupTimeout)
					defer cancel()
					_ = m.resolver.Lookup(lookupCtx, instance, m.cfg.ServiceGroup, "local.", entries)
					m.removeCache.Store(instance, "")
				}(instance)
			case <-ctx.Done():
				return
			}
		}
	}()

	return m.resolver.Browse(ctx, m.cfg.ServiceGroup, "local.", entries)
}

func (m *MDNSDiscovery) Stop(ctx framework.Context) error {
	for _, server := range m.server {
		server.Shutdown()
	}
	close(m.lookupCh)
	m.server = nil
	m.resolver = nil
	return nil
}

func (m *MDNSDiscovery) Register(info *discovery.Info) error {
	if info == nil {
		return fmt.Errorf("service info is nil")
	}
	info.EnsureData()
	server, err := zeroconf.Register(info.Instance,
		m.cfg.ServiceGroup,
		"local.",
		info.LocalPort,
		[]string{
			"version=" + info.Version,
			"protocol=" + info.Protocol,
			"public=" + info.PublicAddr,
			"name=" + info.Name},
		nil)
	if err != nil {
		return err
	}
	m.server[info.Instance] = server
	return nil
}

func (m *MDNSDiscovery) Find(name string) (*discovery.Info, error) {
	// 从缓存中查找服务信息
	if info, found := m.infoCache.Load(name); found {
		// stale-while-revalidate: return quickly, refresh in background.
		timeDiff := time.Since(info.DtUpdate)
		if timeDiff > purgeAfter {
			m.infoCache.Delete(info.Instance)
			return nil, fmt.Errorf("service expired: %s", name)
		}
		if timeDiff > staleAfter {
			if _, loaded := m.lookupMap.LoadOrStore(info.Instance, struct{}{}); !loaded {
				select {
				case m.lookupCh <- info.Instance:
				default:
					// Queue is full, drop refresh signal and allow later retries.
					m.lookupMap.Delete(info.Instance)
				}
			}
		}
		return info, nil
	}
	return nil, fmt.Errorf("service not found: %s", name)
}
