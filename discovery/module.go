package discovery

import (
	"strings"
	"time"

	"github.com/xyzj/toolbox/cache"
)

var (
	discoveryInterval = time.Second * 30
)

func DiscoveryInterval() time.Duration {
	return discoveryInterval
}

type Info struct {
	SourceIP   []string `json:"source"`      // 服务来源IP地址列表
	PublicAddr string   `json:"public_addr"` // 服务地址
	Protocol   string   `json:"protocol"`    // 协议类型，如 "http"、"tcp" 等
	Name       string   `json:"name"`        // 服务名称
	Version    string   `json:"version"`     // 服务版本
	LocalPort  int      `json:"port"`        // 服务端口
}

func (i *Info) Target() string {
	return i.Protocol + "://" + i.PublicAddr
}

func (i *Info) String() string {
	return i.Name + "@" + i.Target() + " (version: " + i.Version + "), from " + strings.Join(i.SourceIP, ",")
}

type ServiceInfo struct {
	data *cache.AnyCache[*Info]
}

func (si *ServiceInfo) Store(info *Info) error {
	return si.data.Store(info.Name, info)
}
func (si *ServiceInfo) Load(name string) (*Info, bool) {
	var info *Info
	si.data.ForEach(func(key string, value *Info) bool {
		if value.Name == name {
			info = value
			return false
		}
		return true
	})
	return info, info != nil
}

func NewServiceInfo() *ServiceInfo {
	return &ServiceInfo{
		data: cache.NewAnyCache[*Info](discoveryInterval + time.Second*5),
	}
}

type Discovery interface {
	Register(info *Info) error
	Find(name string) (*Info, error)
}
