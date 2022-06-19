package logger

import (
	"context"
	"log"
)

//Start 启动logger routine
func Start() (err error, shutdown func()) {
	ctx, cancel := context.WithCancel(context.TODO())
	shutdown = func() {
		log.Println("Shuting down logger")
		cancel()
	}
	go run(ctx)
	//这里需要等待服务器成功建立后再完成
	return
}

func run(ctx context.Context) {

}
func Error(obj interface{}) {
	log.Println(obj)
}
