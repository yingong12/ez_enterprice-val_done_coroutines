package library

// SIGUSR1 toggle the pause/resume consumption
import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/Shopify/sarama"
)

// Sarama configuration options
type Config struct {
	Brokers  string
	Version  string
	Group    string
	Topics   string
	Assignor string
	Oldest   bool
	Verbose  bool
	Consumer *sarama.Config
}

type Consumer struct {
	ready       chan bool
	config      *sarama.Config
	handlerFunc func(*sarama.ConsumerMessage)
}

func Start(msgHandler func(*sarama.ConsumerMessage), config Config, dispatcherDone chan struct{}) (err error, close func() error) {
	log.Println("Starting a new Sarama consumer")
	if config.Verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	version, err := sarama.ParseKafkaVersion(config.Version)
	if err != nil {
		log.Panicf("Error parsing Kafka version: %v", err)
	}

	/**
	 * Construct a new Sarama configuration.
	 * The Kafka cluster version has to be defined before the consumer/producer is initialized.
	 */
	// config := sarama.NewConfig()
	config.Consumer.Version = version

	config.Consumer.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategySticky
	/**
	 * Setup a new Sarama consumer group
	 */
	consumer := Consumer{
		ready:       make(chan bool),
		handlerFunc: msgHandler,
	}

	client, err := sarama.NewConsumerGroup(strings.Split(config.Brokers, ","), config.Group, config.Consumer)
	if err != nil {
		log.Panicf("Error creating consumer group client: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer func() { dispatcherDone <- struct{}{} }()
		for {
			// `Consume` should be called inside an infinite loop, when a
			// server-side rebalance happens, the consumer session will need to be
			// recreated to get the new claims
			if err := client.Consume(ctx, strings.Split(config.Topics, ","), &consumer); err != nil {
				panic(err)
			}
			// check if context was cancelled, signaling that the consumer should stop
			if ctx.Err() != nil {
				return
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready // Await till the consumer has been set up
	log.Println("Sarama consumer up and running!...")

	close = func() error {
		cancel()
		return client.Close()
	}
	return
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Println("Resuming consumption")
	} else {
		client.PauseAll()
		log.Println("Pausing consumption")
	}

	*isPaused = !*isPaused
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/main/consumer_group.go#L27-L29
	for {
		select {
		case message := <-claim.Messages():
			log.Println("拉取消息", string(message.Value))
			consumer.handlerFunc(message)
			//TODO: 先酱， 后面改手动提交
			session.MarkMessage(message, "")

		// Should return when `session.Context()` is done.
		// If not, will raise `ErrRebalanceInProgress` or `read tcp <ip>:<port>: i/o timeout` when kafka rebalance. see:
		// https://github.com/Shopify/sarama/issues/1192
		case <-session.Context().Done():
			return nil
		}
	}
}
