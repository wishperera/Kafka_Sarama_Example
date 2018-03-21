package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

const (
	topic = "my_topic"
)

var msg_count int = 0

func main() {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	//  kafka brokers list, assuming local kafka instance
	brokers := []string{"localhost:9092"}

	leader, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		fmt.Println("Error creating a new consumer :", err)
	}

	// create consumer and define topic, partition and offset to read from
	// setting offset to `OffsetOldest` reads from the beginning
	consumer, err := leader.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		fmt.Println("Error while attempting to create consumer ", err)
	}

	key_interr := make(chan os.Signal, 1)
	signal.Notify(key_interr, os.Interrupt)

	done := make(chan int, 1)
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				msg_count++
				fmt.Println("key: " + string(msg.Key) + "value: " + string(msg.Value))

			case <-key_interr:
				fmt.Println("Exiting consumer process ...")
				done <- 1
			}
		}
	}()

	<-done
	fmt.Println("msg_count: ", msg_count)
}
