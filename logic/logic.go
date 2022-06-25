package logic

import (
	"encoding/json"
	"sync"
	"val_done_coroutines/library"
	"val_done_coroutines/library/env"
	"val_done_coroutines/providers"

	"github.com/Shopify/sarama"
)

type MsgData struct {
	TaskID    string `json:"task_id"`
	FilePath  string `json:"file_path"`
	Age       string `json:"age"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
}

//
type ConsumerError struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Err       string `json:"err"`
}

func Start() (err error, stop func()) {
	dtChan := make(chan MsgData)
	workerWG := sync.WaitGroup{}
	workerWG.Add(50)
	var closeDispatcher func() error
	dispatcherDone := make(chan struct{})
	stop = func() {
		//关闭dispatcher
		closeDispatcher()
		<-dispatcherDone
		//关闭chanel
		close(dtChan)
		//关闭worker
		workerWG.Wait()
	}
	//kafka
	handler := func(msg *sarama.ConsumerMessage) {
		//业务
		dt := &MsgData{}
		err := json.Unmarshal(msg.Value, dt)
		dt.Partition = msg.Partition
		dt.Offset = msg.Offset

		if err != nil {
			return
		}
		//下发消息给worker
		dtChan <- *dt
	}
	config := library.Config{
		Brokers:  env.GetStringVal("KAFKA_BROKERS_VALUATE"),
		Group:    env.GetStringVal("KAFKA_GROUP_VALUATE"),
		Topics:   env.GetStringVal("KAKFA_TOPICS_VALUATE"),
		Version:  "1.2.0",
		Consumer: sarama.NewConfig(),
	}
	//dispatcher
	_, closeDispatcher = library.Start(handler, config, dispatcherDone)
	//启动worker协程
	errChan := make(chan ConsumerError, 5000)
	worker(dtChan, &workerWG, errChan)
	return
}

type ENT struct {
	State uint8  `gorm:"column:state"`
	Path  string `gorm:"column:file_path"`
}

const STATE_SUCCEED = 1

func worker(dataChan chan MsgData, wg *sync.WaitGroup, errChan chan ConsumerError) {
	//50个worker
	for i := 0; i < 50; i++ {
		go func(workerID int) {
			defer func() {
				wg.Done()
			}()
			for dt := range dataChan {
				//业务
				en := ENT{
					//成功
					State: STATE_SUCCEED,
					Path:  dt.FilePath,
				}
				tx := providers.DBAccount.Table("t_valuates")
				tx.Where("valuate_id", dt.TaskID).
					Updates(en)
				if tx.Error != nil {
					errChan <- ConsumerError{
						Partition: dt.Partition,
						Offset:    dt.Offset,
						Err:       tx.Error.Error(),
					}
					return
				}
				tx.Commit()
			}

		}(i)
	}

}
