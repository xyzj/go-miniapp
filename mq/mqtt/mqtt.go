package mqtt

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/xyzj/gominiapp/framework"
	"github.com/xyzj/gominiapp/mq"
	"github.com/xyzj/toolbox"
	"github.com/xyzj/toolbox/crypto"
	mqtt "github.com/xyzj/toolbox/mq"
)

// 1. 定义该插件特有的配置结构体（支持嵌套，字段名与 YAML 对应）

type TLSConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
	RootCA   string `mapstructure:"root_ca"`
}
type Config struct {
	Broker   string    `mapstructure:"broker" yaml:"broker"` // MQTT 服务器地址，支持 tcp:// 和 tls:// 协议
	ClientID string    `mapstructure:"client_id" yaml:"client_id"`
	Username string    `mapstructure:"username" yaml:"username"`
	Password string    `mapstructure:"password" yaml:"password"`
	TLS      TLSConfig `mapstructure:"tls" yaml:"tls"` // 嵌套 TLS 证书路径
}

type MQTTModule struct {
	cfg  Config             // 持有自己的配置
	cli  *mqtt.MqttClientV5 // MQTT 客户端实例
	opt  *Option            // 可选项（如订阅主题和处理函数）
	name string
}

type ModuleOptions func(*MQTTModule)

func (m *MQTTModule) WithName(name string) *MQTTModule {
	m.name = name
	return m
}

func New(opt ...ModuleOptions) *MQTTModule {
	m := &MQTTModule{
		name: "mqtt",
		cfg:  Config{}, // 初始化配置以避免 nil 引用
		opt: &Option{
			topic:   make(map[string]byte),
			handler: func(topic string, payload []byte) {}, // 默认空处理函数
		}, // 初始化 Option 以避免 nil 引用
	}
	for _, o := range opt {
		o(m)
	}
	return m
}
func (m *MQTTModule) Name() string { return m.name }

func (m *MQTTModule) Health() error {
	if m.cli == nil {
		return fmt.Errorf("mqtt client is not initialized")
	}
	if !m.cli.IsConnectionOpen() {
		return fmt.Errorf("mqtt client is not connected")
	}
	return nil
}

func (m *MQTTModule) Init(reg framework.Registry) error {
	if reg == nil {
		return fmt.Errorf("registry is nil")
	}
	// 2. 核心步骤：向框架的注册表“申请”属于我这个插件 Name 的配置切片
	// 它会自动去 YAML 寻找 "mqtt:" 节点并填满 m.cfg
	if err := reg.UnmarshalKey(m.name, &m.cfg); err != nil {
		return err
	}
	if m.cfg.Broker == "" {
		m.cfg = Config{
			Broker:   "tls://localhost:1881",
			ClientID: "goapp_" + toolbox.GetUUID1(),
			Username: "user",
			Password: "pass",
			TLS: TLSConfig{
				Enabled:  true,
				CertFile: "",
				KeyFile:  "",
				RootCA:   "",
			},
		}
		reg.MergeConfig(m.name, m.cfg)
	}
	if m.opt == nil {
		m.opt = &Option{
			topic:   make(map[string]byte),
			handler: func(topic string, payload []byte) {}, // 默认空处理函数
		}
	}
	return nil
}

func (m *MQTTModule) Start(ctx framework.Context) error {
	var tlsc *tls.Config
	var err error
	if strings.HasPrefix(m.cfg.Broker, "tls://") {
		tlsc = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	if m.cfg.TLS.Enabled && m.cfg.TLS.CertFile != "" && m.cfg.TLS.KeyFile != "" {
		tlsc, err = crypto.TLSConfigFromFile(m.cfg.TLS.CertFile, m.cfg.TLS.KeyFile, m.cfg.TLS.RootCA)
		if err != nil {
			ctx.Logger().Error("[MQTT] TLS configuration load failed: " + err.Error())
			return err
		}
	}
	m.cli, err = mqtt.NewMqttClientV5(
		&mqtt.MqttOpt{
			Addr:      m.cfg.Broker,
			ClientID:  m.cfg.ClientID,
			Username:  m.cfg.Username,
			Passwd:    framework.DeobfuscatePassword(m.cfg.Password),
			Logg:      ctx.Logger(),
			Subscribe: m.opt.topic,
			TLSConf:   tlsc,
		},
		m.opt.handler)
	if err != nil {
		ctx.Logger().Error("[MQTT] Client connection failed: " + err.Error())
		return err
	}
	ctx.Provide(m.name, m.cli)
	return nil
}
func (m *MQTTModule) Stop(ctx framework.Context) error {
	if m.cli == nil {
		return nil
	}
	m.cli.Close()
	m.cli = nil
	return nil
}

func (m *MQTTModule) Write(topic string, payload []byte, opts ...mq.Options) error {
	if m.cli == nil {
		return fmt.Errorf("mqtt client is not initialized")
	}
	opt := mq.NewOption()
	for _, o := range opts {
		o(opt)
	}
	return m.cli.Write(topic, payload, mqtt.WithQos(byte(opt.Qos)), mqtt.WithExpire(opt.Expire))
}
