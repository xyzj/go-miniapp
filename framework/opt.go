package framework

import (
	gocmd "github.com/xyzj/go-cmd"
	"github.com/xyzj/toolbox/crypto"
	"github.com/xyzj/toolbox/logger"
)

type frameworkOption struct {
	logger  logger.Logger // 日志记录器
	version *gocmd.Info   // 进程信息
	debug   bool          // 是否开启debug模式
}

type FrameworkOption func(*frameworkOption)

// WithVersion 设置进程信息（如版本号）
func WithVersion(info *gocmd.Info) FrameworkOption {
	return func(opt *frameworkOption) {
		opt.version = info
	}
}

func DeobfuscatePassword(pwd string) string {
	p := crypto.DeobfuscationString(pwd)
	if p == "" {
		return pwd
	}
	return p
}
