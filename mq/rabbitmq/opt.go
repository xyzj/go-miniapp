package rabbitmq

type Option struct {
	topic   []string                           // MQTT 的 Topic
	handler func(topic string, payload []byte) // 业务处理逻辑
}

func WithSubscription(topics []string, handler func(topic string, payload []byte)) Options {
	if len(topics) == 0 || handler == nil {
		topics = make([]string, 0)                      // 避免 nil 引用
		handler = func(topic string, payload []byte) {} // 默认空处理函数
	}
	return func(m *RabbitMQModule) {
		if m.opt == nil {
			m.opt = &Option{
				topic:   make([]string, 0),
				handler: func(topic string, payload []byte) {},
			}
		}
		m.opt.topic = topics
		m.opt.handler = handler
	}
}
