package rabbitmq

import (
	"fmt"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/xyzj/miniapp/framework"
	"github.com/xyzj/miniapp/mq"
)

const (
	amqpURIFormat  = "amqp://%s:%s@%s/%s"
	amqpsURIFormat = "amqps://%s:%s@%s/%s"
)

type TLSConfig struct {
	Enabled  bool   `mapstructure:"enabled"`
	CertFile string `mapstructure:"cert_file"`
	KeyFile  string `mapstructure:"key_file"`
	RootCA   string `mapstructure:"root_ca"`
}
type Config struct {
	Broker             string    `mapstructure:"broker" yaml:"broker"` // RabbitMQ 服务器地址
	Username           string    `mapstructure:"username" yaml:"username"`
	Password           string    `mapstructure:"password" yaml:"password"`
	VHost              string    `mapstructure:"vhost" yaml:"vhost"`                               // vhost名称
	ExchangeName       string    `mapstructure:"exchange_name" yaml:"exchange_name"`               // 交换机名称
	QueueName          string    `mapstructure:"queue_name" yaml:"queue_name"`                     // 队列名
	QueueMaxLength     int       `mapstructure:"queue_max_length" yaml:"queue_max_length"`         // 队列最大长度
	QueueMessageTTL    int       `mapstructure:"queue_message_ttl" yaml:"queue_message_ttl"`       // 队列消息过期时间（毫秒）
	QueueDurable       bool      `mapstructure:"queue_durable" yaml:"queue_durable"`               // 队列是否持久化
	QueueAutoDelete    bool      `mapstructure:"queue_auto_delete" yaml:"queue_auto_delete"`       // 队列在不用时是否删除
	ExchangeDurable    bool      `mapstructure:"exchange_durable" yaml:"exchange_durable"`         // 交换机是否持久化
	ExchangeAutoDelete bool      `mapstructure:"exchange_auto_delete" yaml:"exchange_auto_delete"` // 交换机在不用时是否删除
	TLS                TLSConfig `mapstructure:"tls" yaml:"tls"`                                   // 嵌套 TLS 证书路径
}

type RabbitMQModule struct {
	cfg  Config
	opt  *Option
	conn *amqp.Connection
	ch   *amqp.Channel
	name string
}

type ModuleOptions func(*RabbitMQModule)

func (m *RabbitMQModule) WithName(name string) *RabbitMQModule {
	m.name = name
	return m
}

func New(opt ...ModuleOptions) *RabbitMQModule {
	m := &RabbitMQModule{
		name: "rabbitmq",
		conn: nil,
		ch:   nil,
		cfg:  Config{}, // 初始化配置以避免 nil 引用
		opt: &Option{
			topic:   make([]string, 0),
			handler: func(topic string, payload []byte) {}, // 默认空处理函数
		}, // 初始化 Option 以避免 nil 引用
	}
	for _, o := range opt {
		o(m)
	}
	return m
}

func (m *RabbitMQModule) Name() string { return m.name }

func (m *RabbitMQModule) Health() error {
	if m.conn == nil {
		return fmt.Errorf("rabbitmq connection is not initialized")
	}
	if m.conn.IsClosed() {
		return fmt.Errorf("rabbitmq connection is closed")
	}
	if m.ch == nil {
		return fmt.Errorf("rabbitmq channel is not initialized")
	}
	if m.ch.IsClosed() {
		return fmt.Errorf("rabbitmq channel is closed")
	}
	return nil
}

func (m *RabbitMQModule) Init(reg framework.Registry) error {
	if reg == nil {
		return fmt.Errorf("registry is nil")
	}
	if err := reg.UnmarshalKey(m.name, &m.cfg); err != nil {
		return err
	}
	if m.cfg.Broker == "" {
		m.cfg = Config{
			Broker:             "127.0.0.1:5672",
			Username:           "guest",
			Password:           "guest",
			VHost:              "/",
			ExchangeName:       "app_exchange",
			QueueName:          "app_queue",
			QueueMaxLength:     1000,
			QueueMessageTTL:    60000, // 1分钟
			QueueDurable:       false,
			QueueAutoDelete:    true,
			ExchangeDurable:    false,
			ExchangeAutoDelete: true,
			TLS: TLSConfig{
				Enabled:  false,
				CertFile: "",
				KeyFile:  "",
				RootCA:   "",
			},
		}
		if err := reg.MergeConfig(m.name, m.cfg); err != nil {
			return err
		}
	}
	return nil
}

func (m *RabbitMQModule) Start(ctx framework.Context) error {
	var connStr string
	if m.cfg.TLS.Enabled {
		connStr = fmt.Sprintf(amqpsURIFormat, m.cfg.Username, framework.DeobfuscatePassword(m.cfg.Password), m.cfg.Broker, m.cfg.VHost)
	} else {
		connStr = fmt.Sprintf(amqpURIFormat, m.cfg.Username, framework.DeobfuscatePassword(m.cfg.Password), m.cfg.Broker, m.cfg.VHost)
	}
	conn, err := amqp.Dial(connStr)
	if err != nil {
		ctx.Logger().Error("[RABBITMQ] connect failed: " + err.Error())
		return err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		ctx.Logger().Error("[RABBITMQ] create channel failed: " + err.Error())
		return err
	}
	if len(m.opt.topic) > 0 {
		_, err = ch.QueueDeclare(
			m.cfg.QueueName,
			m.cfg.QueueDurable,
			m.cfg.QueueAutoDelete,
			false,
			false,
			amqp.Table{
				amqp.QueueMaxLenArg:     m.cfg.QueueMaxLength,
				amqp.QueueMessageTTLArg: m.cfg.QueueMessageTTL,
			},
		)
		if err != nil {
			ctx.Logger().Error("[RABBITMQ] declare queue failed: " + err.Error())
			return err
		}
		for _, topic := range m.opt.topic {
			if err = ch.QueueBind(
				m.cfg.QueueName,
				topic,
				m.cfg.ExchangeName,
				false,
				nil,
			); err != nil {
				ctx.Logger().Error("[RABBITMQ] bind queue failed: " + err.Error())
				return err
			}
		}
	}
	m.conn = conn
	m.ch = ch
	ctx.Provide(m.name, m)
	return nil
}

func (m *RabbitMQModule) Stop(ctx framework.Context) error {
	if m.ch != nil && !m.ch.IsClosed() {
		if err := m.ch.Close(); err != nil {
			ctx.Logger().Error("[RABBITMQ] close channel failed: " + err.Error())
			return err
		}
	}
	if m.conn != nil && !m.conn.IsClosed() {
		if err := m.conn.Close(); err != nil {
			ctx.Logger().Error("[RABBITMQ] close connection failed: " + err.Error())
			return err
		}
	}
	return nil
}

func (c *RabbitMQModule) Write(topic string, payload []byte, opts ...mq.Options) error {
	if c.ch == nil {
		return amqp.ErrClosed
	}
	opt := mq.NewOption()
	for _, o := range opts {
		o(opt)
	}
	dm := amqp.Transient
	if opt.Qos == 1 {
		dm = amqp.Persistent
	}
	return c.ch.Publish(
		c.cfg.ExchangeName, // exchange
		topic,              // routing key (queue name)
		false,              // mandatory
		false,              // immediate
		amqp.Publishing{
			ContentType:  "application/octet-stream",
			Body:         payload,
			DeliveryMode: dm,
			Timestamp:    time.Now(),
			Expiration:   strconv.FormatInt(opt.Expire.Milliseconds(), 10), // 设置消息过期时间
		},
	)
}
