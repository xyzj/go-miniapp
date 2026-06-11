package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	rediscli "github.com/redis/go-redis/v9"
	"github.com/xyzj/miniapp/discovery"
	"github.com/xyzj/miniapp/framework"
)

const (
	defaultRedisModuleName = "redis"
	scanCount              = int64(100)
)

type Module struct {
	name string
	cfg  discovery.Config

	redisName string
	cli       *rediscli.Client

	infoCache *discovery.ServiceInfo
	localRegs *discovery.ServiceInfo

	runCtx context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}
type Options func(*Module)

func WithRedisCliName(name string) Options {
	return func(m *Module) {
		m.redisName = name
	}
}

func New(opts ...Options) *Module {
	m := &Module{name: discovery.DefaultModuleName, redisName: defaultRedisModuleName, cfg: discovery.Config{}}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *Module) Name() string { return m.name }

func (m *Module) Health() error {
	if m.cli == nil {
		return fmt.Errorf("redis discovery client is not initialized")
	}
	pingCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return m.cli.Ping(pingCtx).Err()
}

func (m *Module) Init(reg framework.Registry) error {
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
	m.infoCache = discovery.NewServiceInfo(discovery.DiscoveryInterval()*2 + 10*time.Second)
	m.localRegs = discovery.NewServiceInfo(0)
	return nil
}

func (m *Module) Start(ctx framework.Context) error {
	ins, ok := ctx.Get(m.redisName)
	if !ok {
		return fmt.Errorf("redis dependency is not provided: %s", m.redisName)
	}
	cli, ok := ins.(*rediscli.Client)
	if !ok || cli == nil {
		return fmt.Errorf("invalid redis dependency type for %s", m.redisName)
	}
	m.cli = cli
	m.runCtx, m.cancel = context.WithCancel(ctx)

	m.wg.Go(func() {
		m.registerLoop(ctx)
	})
	// m.wg.Go(func() {
	// 	m.scanLoop(ctx)
	// })

	ctx.Provide(m.name, m)
	return nil
}

func (m *Module) Stop(ctx framework.Context) error {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	m.cli = nil
	m.infoCache = nil
	m.localRegs = nil
	return nil
}
func (m *Module) DependsOn() []string {
	return []string{m.redisName}
}

func (m *Module) Register(info *discovery.Info) error {
	if info == nil {
		return fmt.Errorf("service info is nil")
	}
	if info.Instance == "" || info.Instance == info.Name {
		info.Instance = info.Name + "_" + strconv.Itoa(time.Now().Nanosecond())
	}
	info.EnsureData()
	if err := m.localRegs.Store(info); err != nil {
		return err
	}
	return nil
}

func (m *Module) Find(name string) (*discovery.Info, error) {
	if info, ok := m.infoCache.Load(name); ok {
		return info, nil
	}
	return nil, fmt.Errorf("service not found: %s", name)
}

func (m *Module) registerLoop(ctx framework.Context) {
	ticker := time.NewTicker(discovery.DiscoveryInterval())
	defer ticker.Stop()
	m.publishAll(ctx)
	m.refreshFromRedis(ctx)
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			m.publishAll(ctx)
			m.refreshFromRedis(ctx)
		}
	}
}

func (m *Module) scanLoop(ctx framework.Context) {
	ticker := time.NewTicker(discovery.DiscoveryInterval())
	defer ticker.Stop()
	m.refreshFromRedis(ctx)
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			m.refreshFromRedis(ctx)
		}
	}
}

func (m *Module) publishAll(ctx framework.Context) {
	if m.cli == nil || m.localRegs == nil {
		return
	}
	infos := m.localRegs.All()
	if len(infos) == 0 {
		return
	}
	cmdCtx, cancel := context.WithTimeout(m.runCtx, 5*time.Second)
	defer cancel()
	ttl := discovery.DiscoveryInterval()*3 + 5*time.Second
	for _, info := range infos {
		if info == nil {
			continue
		}
		body, err := json.Marshal(info)
		if err != nil {
			ctx.Logger().Error("[DISCOVERY-REDIS] marshal register info failed: " + err.Error())
			continue
		}
		if err := m.cli.Set(cmdCtx, m.discoveryPrefix()+info.Instance, body, ttl).Err(); err != nil {
			ctx.Logger().Error("[DISCOVERY-REDIS] set register key failed: " + err.Error())
		}
	}
}

func (m *Module) refreshFromRedis(ctx framework.Context) {
	if m.cli == nil || m.infoCache == nil {
		return
	}
	cmdCtx, cancel := context.WithTimeout(m.runCtx, 5*time.Second)
	defer cancel()
	var cursor uint64
	pattern := m.discoveryPrefix() + "*"
	for {
		keys, next, err := m.cli.Scan(cmdCtx, cursor, pattern, scanCount).Result()
		if err != nil {
			ctx.Logger().Error("[DISCOVERY-REDIS] scan keys failed: " + err.Error())
			return
		}
		for _, key := range keys {
			raw, err := m.cli.Get(cmdCtx, key).Bytes()
			if err != nil {
				if err != rediscli.Nil {
					ctx.Logger().Error("[DISCOVERY-REDIS] get key failed: " + err.Error())
				}
				continue
			}
			var info discovery.Info
			if err := json.Unmarshal(raw, &info); err != nil {
				ctx.Logger().Error("[DISCOVERY-REDIS] unmarshal key value failed: " + err.Error())
				continue
			}
			if info.Instance == "" {
				continue
			}
			_ = m.infoCache.Store(&info)
		}
		cursor = next
		if cursor == 0 {
			break
		}
	}
}

func (m *Module) discoveryPrefix() string {
	return m.cfg.ServiceGroup + "/discovery/"
}
