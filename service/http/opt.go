package http

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/xyzj/toolbox/logger"
)

type option struct {
	log     logger.Logger
	cors    cors.Config
	handler *gin.Engine
}

type Options func(*option)

func WithCORS(corsConfig cors.Config) Options {
	return func(opt *option) {
		opt.cors = corsConfig
	}
}

func WithHandler(handler *gin.Engine) Options {
	return func(opt *option) {
		opt.handler = handler
	}
}
