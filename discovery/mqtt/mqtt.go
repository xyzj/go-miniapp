package mqtt

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xyzj/miniapp/discovery"
	"github.com/xyzj/miniapp/framework"
	mqmqtt "github.com/xyzj/miniapp/mq/mqtt"
	"github.com/xyzj/toolbox/crypto"
	toolboxmq "github.com/xyzj/toolbox/mq"
)

const defaultMQTTName = "mqtt"

type Module struct {
	name string
	cfg  discovery.Config

	mqttName string
	mqttCfg  mqmqtt.Config
	cli      *toolboxmq.MqttClientV5

	infoCache *discovery.ServiceInfo
	localRegs *discovery.ServiceInfo

	runCtx context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

type Options func(*Module)

func WithMQTTName(name string) Options {
	return func(m *Module) {
		m.mqttName = name
	}
}

func New(opts ...Options) *Module {
	m := &Module{
		name:     discovery.DefaultModuleName,
		cfg:      discovery.Config{},
		mqttName: defaultMQTTName,
		mqttCfg:  mqmqtt.Config{},
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

func (m *Module) Name() string { return m.name }

func (m *Module) Health() error {
	if m.cli == nil {
		return fmt.Errorf("mqtt discovery client is not initialized")
	}
	if !m.cli.IsConnectionOpen() {
		return fmt.Errorf("mqtt discovery client is not connected")
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
	if m.cfg.ServiceGroup == "" {
		m.cfg.ServiceGroup = discovery.DefaultServiceGroup
		if err := reg.MergeConfig(m.name, m.cfg); err != nil {
			return err
		}
	}
	if err := reg.UnmarshalKey(m.mqttName, &m.mqttCfg); err != nil {
		return err
	}
	m.infoCache = discovery.NewServiceInfo(discovery.DiscoveryInterval()*2 + 10*time.Second)
	m.localRegs = discovery.NewServiceInfo(0)
	return nil
}

func (m *Module) Start(ctx framework.Context) error {
	var tlsCfg *tls.Config
	var err error
	if strings.HasPrefix(m.mqttCfg.Broker, "tls://") {
		tlsCfg = &tls.Config{InsecureSkipVerify: true}
	}
	if m.mqttCfg.TLS.Enabled && m.mqttCfg.TLS.CertFile != "" && m.mqttCfg.TLS.KeyFile != "" {
		tlsCfg, err = crypto.TLSConfigFromFile(m.mqttCfg.TLS.CertFile, m.mqttCfg.TLS.KeyFile, m.mqttCfg.TLS.RootCA)
		if err != nil {
			ctx.Logger().Error("[DISCOVERY-MQTT] TLS configuration load failed: " + err.Error())
			return err
		}
	}

	m.cli, err = toolboxmq.NewMqttClientV5(
		&toolboxmq.MqttOpt{
			Addr:      m.mqttCfg.Broker,
			ClientID:  m.mqttCfg.ClientID + "_discovery",
			Username:  m.mqttCfg.Username,
			Passwd:    framework.DeobfuscatePassword(m.mqttCfg.Password),
			Logg:      ctx.Logger(),
			Subscribe: map[string]byte{m.discoveryPrefix() + "#": 0},
			TLSConf:   tlsCfg,
			LogHeader: "[DISCOVERY-MQTT]",
		},
		func(topic string, payload []byte) {
			m.onMessage(ctx, topic, payload)
		},
	)
	if err != nil {
		ctx.Logger().Error("[DISCOVERY-MQTT] Client connection failed: " + err.Error())
		return err
	}

	m.runCtx, m.cancel = context.WithCancel(ctx)
	m.wg.Go(func() {
		m.publishLoop(ctx)
	})

	ctx.Provide(m.name, m)
	return nil
}

func (m *Module) Stop(ctx framework.Context) error {
	if m.cancel != nil {
		m.cancel()
	}
	m.wg.Wait()
	if m.cli != nil {
		if err := m.cli.Close(); err != nil {
			ctx.Logger().Error("[DISCOVERY-MQTT] close failed: " + err.Error())
			return err
		}
		m.cli = nil
	}
	m.infoCache = nil
	m.localRegs = nil
	return nil
}
func (m *Module) DependsOn() []string {
	return []string{m.mqttName}
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

func (m *Module) publishLoop(ctx framework.Context) {
	ticker := time.NewTicker(discovery.DiscoveryInterval())
	defer ticker.Stop()
	m.publishAll(ctx)
	for {
		select {
		case <-m.runCtx.Done():
			return
		case <-ticker.C:
			m.publishAll(ctx)
		}
	}
}

func (m *Module) publishAll(ctx framework.Context) {
	if m.cli == nil || m.localRegs == nil {
		return
	}
	infos := m.localRegs.All()
	for _, info := range infos {
		if info == nil {
			continue
		}
		body, err := json.Marshal(info)
		if err != nil {
			ctx.Logger().Error("[DISCOVERY-MQTT] marshal register info failed: " + err.Error())
			continue
		}
		if err := m.cli.Write(m.discoveryPrefix()+info.Instance, body); err != nil {
			ctx.Logger().Error("[DISCOVERY-MQTT] publish register info failed: " + err.Error())
		}
	}
}

func (m *Module) onMessage(ctx framework.Context, topic string, payload []byte) {
	if !strings.HasPrefix(topic, m.discoveryPrefix()) {
		return
	}
	var info discovery.Info
	if err := json.Unmarshal(payload, &info); err != nil {
		ctx.Logger().Error("[DISCOVERY-MQTT] unmarshal discovery message failed: " + err.Error())
		return
	}
	if info.Instance == "" {
		return
	}
	_ = m.infoCache.Store(&info)
}

func (m *Module) discoveryPrefix() string {
	return m.cfg.ServiceGroup + "/discovery/"
}
