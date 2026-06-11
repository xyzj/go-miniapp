package discovery

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xyzj/toolbox"
)

const (
	DefaultServiceGroup = "goapp_group"
	DefaultModuleName   = "discovery"
)

var (
	discoveryInterval = time.Second * 15
)

type DiscoveryType byte

const (
	DiscoveryTypeNone DiscoveryType = iota
	DiscoveryTypeMDNS
	DiscoveryTypeUnixSock
	DiscoveryTypeRedis
	DiscoveryTypeMqtt
)

func DiscoveryInterval() time.Duration {
	return discoveryInterval
}

type Info struct {
	SourceIP   []string  `json:"source"`      // 服务来源IP地址列表
	PublicAddr string    `json:"public_addr"` // 服务地址
	Protocol   string    `json:"protocol"`    // 协议类型，如 "http"、"tcp" 等
	Instance   string    `json:"instance"`    // 服务实例名称,用于区分同一服务组中的不同服务实例,通常为服务名称+发布地址/协议类型等可区分的信息
	Name       string    `json:"name"`        // 服务名称
	Version    string    `json:"version"`     // 服务版本
	LocalPort  int       `json:"port"`        // 服务端口
	DtUpdate   time.Time `json:"-"`           // 信息更新时间
}

func (i *Info) Target() string {
	return i.Protocol + "://" + i.PublicAddr
}

func (i *Info) String() string {
	return i.Instance + "@" + i.Target() + " (version: " + i.Version + "), from " + strings.Join(i.SourceIP, ",")
}

func (i *Info) EnsureData() {
	if len(i.SourceIP) == 0 {
		ips, err := toolbox.GetLocalIPs(true)
		if err == nil {
			i.SourceIP = ips
		} else {
			i.SourceIP = []string{}
		}
	}
	if i.PublicAddr == "" {
		if len(i.SourceIP) > 0 {
			i.PublicAddr = i.SourceIP[0] + ":" + strconv.Itoa(i.LocalPort)
		} else {
			i.PublicAddr = "127.0.0.1:" + strconv.Itoa(i.LocalPort)
		}
	}
	_, p, err := net.SplitHostPort(i.PublicAddr)
	if err != nil {
		n := net.ParseIP(i.PublicAddr)
		if n != nil {
			i.PublicAddr = net.JoinHostPort(i.PublicAddr, fmt.Sprintf("%d", i.LocalPort))
		}
	} else {
		if p == "" || p == "0" {
			i.PublicAddr = net.JoinHostPort(strings.TrimSuffix(i.PublicAddr, ":"), fmt.Sprintf("%d", i.LocalPort))
		}
	}
	if i.Instance == "" {
		i.Instance = i.Name + "_" + i.PublicAddr
	}
}

type ServiceCache struct {
	info     *Info
	interval time.Duration
}

func (sc *ServiceCache) IsExpired() bool {
	if sc.interval.Seconds() <= 0 {
		return false
	}
	return time.Since(sc.info.DtUpdate) > sc.interval
}
func (sc *ServiceCache) Target() string {
	return sc.info.Target()
}
func (sc *ServiceCache) String() string {
	return sc.info.String()
}

type ServiceInfo struct {
	locker       sync.RWMutex
	data         map[string]*ServiceCache
	timeInterval time.Duration
}

func (si *ServiceInfo) Store(info *Info) error {
	si.locker.Lock()
	defer si.locker.Unlock()
	info.DtUpdate = time.Now()
	si.data[info.Instance] = &ServiceCache{
		info:     info,
		interval: si.timeInterval,
	}
	return nil
}
func (si *ServiceInfo) Load(name string) (*Info, bool) {
	si.locker.RLock()
	defer si.locker.RUnlock()
	var info *Info
	for _, value := range si.data {
		if value.IsExpired() {
			continue
		}
		if value.info.Name == name {
			info = value.info
			break
		}
	}
	return info, info != nil
}
func (si *ServiceInfo) Delete(instance string) {
	si.locker.Lock()
	defer si.locker.Unlock()
	delete(si.data, instance)
}

func (si *ServiceInfo) All() []*Info {
	si.locker.RLock()
	defer si.locker.RUnlock()
	infos := make([]*Info, 0, len(si.data))
	for _, value := range si.data {
		if value.IsExpired() {
			continue
		}
		infos = append(infos, value.info)
	}
	return infos
}

func NewServiceInfo(t time.Duration) *ServiceInfo {
	si := &ServiceInfo{
		locker:       sync.RWMutex{},
		data:         make(map[string]*ServiceCache),
		timeInterval: t,
	}
	if t.Seconds() > 0 {
		go func() {
			ticker := time.NewTicker(t)
			defer ticker.Stop()
			for {
				<-ticker.C
				si.locker.Lock()
				for key, cache := range si.data {
					if cache.IsExpired() {
						delete(si.data, key)
					}
				}
				si.locker.Unlock()
			}
		}()
	}
	return si
}

type Discovery interface {
	Register(info *Info) error
	Find(name string) (*Info, error)
}

type Config struct {
	ServiceGroup string `mapstructure:"service_group" yaml:"service_group"` // 服务组名，用于区分一个网络中不同组的服务，如 "myapp-http"
}
