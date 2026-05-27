package framework

import (
	"sync"

	"github.com/spf13/viper"
	gocmd "github.com/xyzj/go-cmd"
)

type App struct {
	modules     map[string]Module
	orderedMods []Module
	instances   sync.Map
	v           *viper.Viper // 统一配置中心
	opt         *frameworkOption
	ctx         Context
	cmd         *gocmd.Program
}

// Registry 接口：让业务或插件能从中“捞取”其他插件的能力
type Registry interface {
	Provide(name string, instance any)
	Get(name string) (any, bool)
	// MustGet(name string) (any, bool)
	UnmarshalKey(key string, rawVal any) error // UnmarshalKey 核心：将配置文件的某个节点（如 "mqtt"）绑定到指定的结构体指针上
	MergeConfig(key string, rawVal any) error  // MergeConfig 将整个配置文件合并到指定结构体指针上，适用于全局配置
}

// Module 基础接口：管理生命周期
type Module interface {
	Name() string
	Health() error           // 健康检查接口，供框架在启动后进行自检
	Init(reg Registry) error // 初始化阶段：允许通过 reg 获取依赖或注册自己的服务接口
	Start(ctx Context) error // 启动阶段：如果是 Server，在此阻塞运行
	Stop(ctx Context) error  // 停止阶段：优雅关闭
}

// Dependable 接口：由需要声明依赖的插件自愿实现
type Dependable interface {
	DependsOn() []string // 返回它所依赖的插件名称列表
}

type logConfig struct {
	LogLevel string `mapstructure:"log_level" yaml:"log_level"` // 日志级别
	LogFile  string `mapstructure:"log_file" yaml:"log_file"`   // 日志文件路径
	LogDays  int    `mapstructure:"log_days" yaml:"log_days"`   // 日志保留天数
}
