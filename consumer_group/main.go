package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/Shopify/sarama"
)

func main() {
	var wg sync.WaitGroup
	config := sarama.NewConfig()
	config.Version = sarama.V0_11_0_2

	client, err := sarama.NewClient([]string{"127.0.0.1:9092"}, config)
	defer client.Close()
	if err != nil {
		panic(err)
	}

	// 基于sarama client创建消费组client
	consumerGroup1, err := sarama.NewConsumerGroupFromClient("cg1", client)
	if err != nil {
		panic(err)
	}
	consumerGroup2, err := sarama.NewConsumerGroupFromClient("cg2", client)
	if err != nil {
		panic(err)
	}
	consumerGroup3, err := sarama.NewConsumerGroupFromClient("cg3", client)
	if err != nil {
		panic(err)
	}

	defer consumerGroup1.Close()
	defer consumerGroup2.Close()
	defer consumerGroup3.Close()

	wg.Add(3)
	go consume(&consumerGroup1, &wg, "cg1")
	go consume(&consumerGroup2, &wg, "cg2")
	go consume(&consumerGroup3, &wg, "cg3")
	wg.Wait()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	select {
	case <-signals:

	}
}

// 要想使用消费组的消费功能，必须声明一个struct，并用这个struct去实现Setup、Cleanup、ConsumeClaim这三个接口的方法
type ConsumerGroupHandler struct {
	name string
}

func (c *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	// NOTE:
	// Do not move the code below to a goroutine.
	// The `ConsumeClaim` itself is called within a goroutine, see:
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)
		session.MarkMessage(message, "")
	}

	return nil
}

func consume(consumerGroup *sarama.ConsumerGroup, wg *sync.WaitGroup, name string) {
	log.Println("consumerGroup :", name, " started ")
	wg.Done()
	for {
		handler := ConsumerGroupHandler{name: name}
		// 指定消费哪个topic
		if err := (*consumerGroup).Consume(context.Background(), []string{"mytopic"}, &handler); err != nil {
			panic(err)
		}
	}
}
