package mqtt

type Option struct {
	topic   map[string]byte                    // MQTT 的 Topic
	handler func(topic string, payload []byte) // 业务处理逻辑
}

func WithSubscription(topics map[string]byte, handler func(topic string, payload []byte)) ModuleOptions {
	if len(topics) == 0 || handler == nil {
		topics = make(map[string]byte)                  // 避免 nil 引用
		handler = func(topic string, payload []byte) {} // 默认空处理函数
	}
	return func(m *MQTTModule) {
		if m.opt == nil {
			m.opt = &Option{
				topic:   make(map[string]byte),
				handler: func(topic string, payload []byte) {},
			}
		}
		m.opt.topic = topics
		m.opt.handler = handler
	}
}
