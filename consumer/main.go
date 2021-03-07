package main

import (
	"log"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
)

func main() {
	// 开启记录消费消息的失败数
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// 先构造一个sarama client
	client, err := sarama.NewClient([]string{"127.0.0.1:9092"}, config)
	defer client.Close()
	if err != nil {
		panic(err)
	}

	// 基于这个sarama client再创建消费者client
	consumer, err := sarama.NewConsumerFromClient(client)
	defer consumer.Close()
	if err != nil {
		panic(err)
	}

	// 指定某个消费者去消费某个topic所有的partition，返回每个partition的ID
	partitionIDs, err := consumer.Partitions("mytopic")
	if err != nil {
		panic(err)
	}

	// 遍历每个partitionID，用消费者为每一个partition构造出一个partitionConsumer
	for _, partitionID := range partitionIDs {
		partitionConsumer, err := consumer.ConsumePartition("mytopic", partitionID, sarama.OffsetNewest)
		if err != nil {
			panic(err)
		}

		// 为每一个partitionConsumer开启一个线程去从channel拿出消息消费
		go func(pc *sarama.PartitionConsumer) {
			defer (*pc).Close()
			for message := range (*pc).Messages() {
				value := string(message.Value)
				log.Println("Partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, value)
			}
		}(&partitionConsumer)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	select {
	case <-signals:

	}
}
