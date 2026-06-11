# miniapp

miniapp 是一个基于 Go 的模块化应用框架示例，支持按插件方式组合数据库、服务发现、消息队列和服务端能力。

## 特性

- 模块化插件注册与生命周期管理（初始化、启动、停止）
- 多种数据库模块（sqlite、mysql、pgsql、redis、sqlserver、bolt）
- 多种发现能力（mdns、mqtt、redis、unixsock）
- 多种消息队列能力（mqtt、rabbitmq）
- 内置服务能力示例（http、tcp）

## 目录说明

- `cmd/`: 主程序入口与默认配置
- `framework/`: 应用框架核心（App、模块注册、上下文等）
- `db/`: 数据库模块
- `discovery/`: 服务发现模块
- `mq/`: 消息队列模块
- `service/`: 服务模块
- `example/`: 生产者/消费者示例

## 环境要求

- Go 1.26+

## 快速开始

1. 拉取依赖

```bash
go mod download
```

2. 按需修改配置文件

- 默认配置文件: `cmd/config.yaml`
- 请将其中示例地址、账号与密码替换为你自己的环境参数

3. 启动主程序

```bash
go run ./cmd
```

4. 健康检查

程序启动后可访问:

```text
GET /ping
```

默认监听端口可在 `cmd/config.yaml` 的 `http.port` 中配置。

## 示例

- 生产者示例: `example/producer`
- 消费者示例: `example/consumer`

可分别进入目录后执行:

```bash
go run .
```

## 说明

- 本仓库以最小示例为主，便于扩展自己的模块。
- 新增模块时，建议保持模块名唯一，并在主程序中显式注册。