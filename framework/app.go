package framework

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/spf13/viper"
	gocmd "github.com/xyzj/go-cmd"
	"github.com/xyzj/toolbox/crypto"
	"github.com/xyzj/toolbox/logger"
)

var (
	configPath = flag.String("config", "./config.yaml", "Path to configuration file")
	debug      = flag.Bool("debug", false, "Enable debug mode")
)

func parseStartupFlags() {
	args := os.Args[1:]
	if len(args) > 0 && len(args[0]) > 0 && args[0][0] != '-' {
		args = args[1:]
	}
	_ = flag.CommandLine.Parse(args)
}

func NewApp(opts ...FrameworkOption) (*App, error) {
	parseStartupFlags()
	app := &App{
		modules:   make(map[string]Module),
		instances: sync.Map{},
		v:         viper.New(),
	}
	// 应用工厂选项
	factoryOpts := &frameworkOption{
		logger:  logger.NewNilLogger(),
		version: &gocmd.Info{Name: "GoMicroApp", Version: "0.0.1"},
		debug:   *debug,
	}
	for _, opt := range opts {
		opt(factoryOpts)
	}
	app.opt = factoryOpts

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	appctx := Context{
		Context: ctx,
		options: app.opt,
		reg:     app,
	}
	app.ctx = appctx
	cmd := gocmd.DefaultProgram(&gocmd.Info{
		Name:        app.opt.version.Name,
		Version:     app.opt.version.Version,
		Description: app.opt.version.Description,
	}).AddCommand(&gocmd.Command{
		Name: "hashpassword",
		RunWithExitCode: func(pi *gocmd.ProcInfo) int {
			var pwd string
			fmt.Print("Enter the password: ")
			fmt.Scanln(&pwd)
			fmt.Println(crypto.ObfuscationString(pwd))
			return 0
		},
	}).AfterStop(func() {
		cancel() // 1. 取消全局上下文，通知所有插件开始优雅关闭
		// 6. 逆序执行 Stop (后启动的先关闭，保护依赖)
		for i := len(app.orderedMods) - 1; i >= 0; i-- {
			m := app.orderedMods[i]
			func(mod Module) {
				app.opt.logger.Info("[Framework] Stopping module: " + mod.Name())
				defer func() {
					if r := recover(); r != nil {
						app.opt.logger.Error(fmt.Sprintf("[Framework] Module [%s] stop panic: %v", mod.Name(), r))
					}
				}()
				if err := mod.Stop(appctx); err != nil {
					app.opt.logger.Error(fmt.Sprintf("[Framework] Module [%s] stop error: %v", mod.Name(), err))
				}
			}(m)
		}
		app.opt.logger.Info("[Framework] Shutting down server gracefully...")
		app.opt.logger.Close()
	})
	app.cmd = cmd
	app.cmd.Execute()
	// 初始化时读取配置文件
	app.v.SetConfigFile(*configPath)
	if err := app.v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	logc := &logConfig{}
	log := logger.NewNilLogger()
	if !app.v.InConfig("log") {
		logc = &logConfig{
			LogLevel: "info",
			LogFile:  "",
			LogDays:  7,
		}
		app.v.MergeConfigMap(map[string]any{
			"log": logc,
		})
		app.v.WriteConfig()
	} else {
		app.UnmarshalKey("log", logc)
	}
	if logc.LogFile == "" {
		log = logger.NewConsoleLogger()
	} else {
		log = logger.NewLogger(logger.GetLevel(logc.LogLevel),
			logger.WithFilename(logc.LogFile),
			logger.WithMaxDays(logc.LogDays))
	}
	if *debug && logc.LogFile != "" {
		log = logger.NewMultiLogger(
			logger.NewConsoleLogger(),
			log,
		)
	}
	app.opt.logger = log
	return app, nil
}

// DefaultLogger 提供给插件使用的日志接口，插件可以通过 app.DefaultLogger() 获取到框架的日志实例
func (a *App) DefaultLogger() logger.Logger {
	if a.opt == nil || a.opt.logger == nil {
		return logger.NewNilLogger()
	}
	return a.opt.logger
}

// Register 注册插件
func (a *App) Register(mods ...Module) {
	for _, m := range mods {
		a.modules[m.Name()] = m
	}
}

// Provide 允许插件将自己的能力（实例）注入到全局上下文中
func (a *App) Provide(name string, instance any) {
	a.instances.Store(name, instance)
}

// Get 实现 Registry 接口，供其他插件在 Init 时调用
func (a *App) Get(name string) (any, bool) {
	ins, ok := a.instances.Load(name)
	return ins, ok
}

// UnmarshalKey 实现 Registry 接口，供插件在 Init 时调用，绑定配置到结构体
func (a *App) UnmarshalKey(key string, rawVal any) error {
	// Viper 会自动寻找 YAML 中对应的 Key，并解析到指定的 struct 中
	return a.v.UnmarshalKey(key, rawVal)
}

func (a *App) MergeConfig(key string, rawVal any) error {
	a.v.MergeConfigMap(map[string]any{key: rawVal})
	a.v.WriteConfig() // 将合并后的配置写回文件
	return nil
}

// Run 核心启动器
func (a *App) Run() error {
	for _, m := range a.modules {
		a.opt.logger.Info("[Framework] Initializing module: " + m.Name())
		if err := m.Init(a); err != nil {
			err := fmt.Errorf("[Framework] module [%s] init failed: %w", m.Name(), err)
			a.opt.logger.Error(err.Error())
			return err
		}
	}
	if err := a.compileTopology(); err != nil {
		return err
	}
	for _, m := range a.orderedMods {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
		go func(mod Module, cancel context.CancelFunc) {
			a.opt.logger.Info("[Framework] Starting module: " + mod.Name())
			defer func() {
				if r := recover(); r != nil {
					a.opt.logger.Error(fmt.Sprintf("[Framework] Module [%s] start panic: %v", mod.Name(), r))
					a.cmd.Exit(1)
				}
			}()
			if err := mod.Start(a.ctx); err != nil {
				a.opt.logger.Error(fmt.Sprintf("[Framework] Module [%s] start error: %v", mod.Name(), err))
				a.cmd.Exit(1)
			}
			cancel()
		}(m, cancel)
		<-ctx.Done()                       // 等待当前模块启动完成后再启动下一个，保证依赖的模块都已启动
		if _, ok := a.Get(m.Name()); !ok { // 如果模块没有主动提供实例，就把模块本身注册到上下文中，供其他模块依赖使用
			a.Provide(m.Name(), m)
		}
	}
	return nil
}

// compileTopology 计算依赖拓扑排序（深度优先搜索实现）
func (a *App) compileTopology() error {
	visited := make(map[string]int) // 0:未访问, 1:访问中, 2:已完成
	var order []Module

	var dfs func(name string) error
	dfs = func(name string) error {
		if visited[name] == 1 {
			return fmt.Errorf("circular dependency detected at module: %s", name)
		}
		if visited[name] == 2 {
			return nil
		}

		visited[name] = 1
		mod, ok := a.modules[name]
		if !ok {
			return fmt.Errorf("required module [%s] was not registered", name)
		}

		// 如果该插件声明了依赖，先访问依赖
		if depMod, ok := mod.(Dependable); ok {
			for _, depName := range depMod.DependsOn() {
				if err := dfs(depName); err != nil {
					return err
				}
			}
		}

		visited[name] = 2
		order = append(order, mod)
		return nil
	}

	for name := range a.modules {
		if visited[name] == 0 {
			if err := dfs(name); err != nil {
				return err
			}
		}
	}

	a.orderedMods = order
	return nil
}
