package mq

import "time"

type Option struct {
	Expire time.Duration
	Qos    byte
}

type Options func(*Option)

func NewOption() *Option {
	return &Option{
		Expire: time.Minute, // 默认消息过期时间1分钟
		Qos:    0,           // 默认QoS等级为0
	}
}

// WithExpire 设置消息的过期时间
func WithExpire(expire time.Duration) Options {
	return func(opt *Option) {
		opt.Expire = expire
	}
}

// WithQos 设置消息的 QoS 等级（0、1 或 2）
func WithQos(qos byte) Options {
	return func(opt *Option) {
		opt.Qos = qos
	}
}

type MQCli interface {
	Write(topic string, payload []byte, opts ...Options) error
}
