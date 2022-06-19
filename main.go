package main

import (
	"log"
	"runtime"
)

func init() {
	runtime.GOMAXPROCS(runtime.NumCPU())
}
func main() {
	/*
		1.配置加载
		2.进程监视。
		3.启动webserver。
		4.日志线程。
	*/
	if err := loadConfs(); err != nil {
		log.Fatal("system crashed while loading confs ", err)
	}
	if err := bootStrap(); err != nil {
		log.Fatal("system crashed... ", err)
	}
}

//loadConfs 将环境变量等加载到对应conf中
func loadConfs() (err error) {
	return
}
