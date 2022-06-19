package library

import (
	"fmt"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/plugin/dbresolver"
)

var GormConfigs []*GormConfig

type GormDB struct {
	*gorm.DB
}

type GormConfig struct {
	Receiver              **GormDB
	ConnectionName        string            //连接名称
	DBName                string            //db名称
	Host                  string            //地址
	Port                  string            //端口
	UserName              string            //用户名
	Password              string            //密码
	MaxLifeTime           int               //空闲连接最大保持时长(秒)
	MaxOpenConn           int               //最大打开连接数
	MaxIdleConn           int               //最大空闲连接数
	EnableTransaction     bool              //是否开启事务
	ReadOnlySlavesConfigs []GormSlaveConfig //只读实例配置
}

type GormSlaveConfig struct {
	Host     string
	Port     string
	UserName string
	Password string
}

const (
	dataSourceNameFormat = "%s:%s@tcp(%s:%s)/%s"
	driverName           = "mysql"
)

func NewGormDB(conf *GormConfig) (*GormDB, error) {
	dsn := fmt.Sprintf(dataSourceNameFormat,
		conf.UserName,
		conf.Password,
		conf.Host,
		conf.Port,
		conf.DBName,
	)

	gormDB, err := gorm.Open(mysql.Open(dsn), &gorm.Config{
		SkipDefaultTransaction: !conf.EnableTransaction,
	})
	if err != nil {
		err = fmt.Errorf("gorm connection:[%s] Open error:%w", conf.ConnectionName, err)
		return nil, err
	}
	//配置了只读库列表
	if len(conf.ReadOnlySlavesConfigs) > 0 {
		var slaveReplicas []gorm.Dialector
		for _, slaveConfig := range conf.ReadOnlySlavesConfigs {
			slaveReplicas = append(slaveReplicas, mysql.Open(fmt.Sprintf(dataSourceNameFormat,
				slaveConfig.UserName,
				slaveConfig.Password,
				slaveConfig.Host,
				slaveConfig.Port,
				conf.DBName,
			)))
		}
		dbResolverCfg := dbresolver.Config{
			Replicas: slaveReplicas,
			Policy:   dbresolver.RandomPolicy{}}
		readWritePlugin := dbresolver.Register(dbResolverCfg).
			SetMaxOpenConns(conf.MaxOpenConn).
			SetMaxIdleConns(conf.MaxIdleConn).
			SetConnMaxIdleTime(time.Duration(conf.MaxLifeTime) * time.Second)
		if err = gormDB.Use(readWritePlugin); err != nil {
			err = fmt.Errorf("database connection:[%s] err: %w", conf.ConnectionName, err)
			return nil, err
		}
	}

	return &GormDB{gormDB}, nil
}
