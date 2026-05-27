package mqtt

type Option struct {
	topic   map[string]byte                    // MQTT 的 Topic
	handler func(topic string, payload []byte) // 业务处理逻辑
}

type MqttOptions func(*Option)

func WithSubscription(topics map[string]byte, handler func(topic string, payload []byte)) MqttOptions {
	if len(topics) == 0 || handler == nil {
		topics = make(map[string]byte)                  // 避免 nil 引用
		handler = func(topic string, payload []byte) {} // 默认空处理函数
	}
	return func(opt *Option) {
		opt.topic = topics
		opt.handler = handler
	}
}
