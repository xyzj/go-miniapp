package framework

import (
	gocmd "github.com/xyzj/go-cmd"
	"github.com/xyzj/toolbox/crypto"
	"github.com/xyzj/toolbox/logger"
)

type option struct {
	logger  logger.Logger // 日志记录器
	version *gocmd.Info   // 进程信息
	debug   bool          // 是否开启debug模式
}

type Options func(*option)

// WithVersion 设置进程信息（如版本号）
func WithVersion(info *gocmd.Info) Options {
	return func(opt *option) {
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
