package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

func main() {
	//
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	// 设置生产者的配置，如生产者发送到partition的策略，一个topic有这么多的partition，
	// 消息就只有一条，怎么知道消息是发送到哪个partition呢，就是通过这个策略，有随机，hash，手动指定等策略
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	// 先构造一个sarama client
	client, err := sarama.NewClient([]string{"127.0.0.1:9092"}, config)
	defer client.Close()
	if err != nil {
		panic(err)
	}

	// 基于这个sarama client再创建生产者client
	producer, err := sarama.NewAsyncProducerFromClient(client)
	defer producer.Close()
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var (
		wg                          sync.WaitGroup
		enqueued, successes, errors int
	)

	wg.Add(1)
	// 开启一个线程，记录生产者生产数据的成功条数
	go func() {
		defer wg.Done()
		for range producer.Successes() {
			successes++
		}
	}()

	wg.Add(1)
	// 开启一个线程，记录生产者生产数据的失败条数
	go func() {
		defer wg.Done()
		for err := range producer.Errors() {
			log.Println("err: ", err)
			errors++
		}
	}()

	log.Println("start to produce message ...")

	// 类似与java的goto, 在循环中跑到指定条件的时候跳出循环
ProducerLoop:
	for {
		// 指定生产消息到哪个topic+生成数据的值，构造消息结构体
		message := &sarama.ProducerMessage{Topic: "mytopic", Value: sarama.StringEncoder("test 123")}
		select {
		// 将消息体放入生产者的channel
		case producer.Input() <- message:
			enqueued++
		// 一旦受到中止信号，如ctrl+C，停止循环
		case <-signals:
			producer.AsyncClose()
			break ProducerLoop
		}
		log.Println("Successfully produced: %d; errors: %d\n", successes, errors)
		time.Sleep(2 * time.Second)
	}

	wg.Wait()
}
