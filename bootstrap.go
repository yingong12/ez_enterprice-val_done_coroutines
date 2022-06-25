package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"val_done_coroutines/library"
	"val_done_coroutines/library/env"
	"val_done_coroutines/logger"
	"val_done_coroutines/logic"
	"val_done_coroutines/providers"

	"github.com/joho/godotenv"
)

//bootstrap providers,以及routines
func bootStrap() (err error) {
	//加载环境变量
	filePath := ".env"
	flag.StringVar(&filePath, "c", ".env", "配置文件")
	flag.Parse()
	if err = godotenv.Load(filePath); err != nil {
		return
	}
	log.Println("env loadded from file ", filePath)

	err, shutdownLogger := logger.Start()
	if err != nil {
		return
	}
	log.Println("Logger Started ")
	//加载Redis连接池
	// DB GORM初始化
	GormConfigs := []*library.GormConfig{
		{
			Receiver:       &providers.DBAccount,
			ConnectionName: "gorm-core",
			DBName:         env.GetStringVal("DB_COMPANY_RW_NAME"),
			Host:           env.GetStringVal("DB_COMPANY_RW_HOST"),
			Port:           env.GetStringVal("DB_COMPANY_RW_PORT"),
			UserName:       env.GetStringVal("DB_COMPANY_RW_USERNAME"),
			Password:       env.GetStringVal("DB_COMPANY_RW_PASSWORD"),
			MaxLifeTime:    env.GetIntVal("DB_MAX_LIFE_TIME"),
			MaxOpenConn:    env.GetIntVal("DB_MAX_OPEN_CONN"),
			MaxIdleConn:    env.GetIntVal("DB_MAX_IDLE_CONN"),
		},
	}

	for _, cfg := range GormConfigs {
		if cfg.Receiver == nil {
			return fmt.Errorf("[%s] config receiver cannot be nil", cfg.ConnectionName)
		}
		if *cfg.Receiver, err = library.NewGormDB(cfg); err != nil {
			return err
		}
		_, e := (*cfg.Receiver).DB.DB()
		if e != nil {
			return e
		}
	}

	//
	_, stop := logic.Start()
	//wait for sys signals
	exitChan := make(chan os.Signal)
	signal.Notify(exitChan, os.Interrupt, os.Kill, syscall.SIGTERM)
	select {
	case sig := <-exitChan:
		stop()
		log.Println("Doing cleaning works before shutdown...")
		shutdownLogger()
		log.Println("You abandoned me, bye bye", sig)
	}
	return
}
