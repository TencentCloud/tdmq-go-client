package main

import (
	"context"
	"fmt"
	"github.com/TencentCloud/tdmq-go-client/pulsar"
	"github.com/sirupsen/logrus"
	"log"
	"os"
	"strconv"
	"time"
	//"github.com/apache/pulsar-client-go/pulsar/internal"
	//"github.com/stretchr/testify/assert"
)

func init() {
	logrus.SetLevel(logrus.WarnLevel)
}

func produceAndConsumerDemo(secretId, secretKey, uin, region, brokerUrl, topic, subname string, num int) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	for i := 0; i < num; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("produceAndConsumerDemo functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published message: %s\n", msgId)
		}

		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func producerAsync(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching:         true,
		Topic:                   topic,
		BatchingMaxPublishDelay: 1 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 10; i++ {
		producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("producerAsync functions-demo"),
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, e error) {
			if e != nil {
				log.Fatal(e)
			} else {
				fmt.Println("Published SendAsync message ", id)
			}
		})
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received SendAsync msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func producerCompression(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
		CompressionType: pulsar.ZLib,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 1; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload:      []byte("producerCompression type ZLib functions-demo" ),
			DeliverAfter: 3 * time.Second,
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published ZLib CompressionType message: %s\n", msgId)
		}
	}

	producer1, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
		CompressionType: pulsar.LZ4,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer1.Close()
	for i := 0; i < 1; i++ {
		if msgId, err := producer1.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("producerCompression type LZ4 functions-demo" ),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published LZ4 CompressionType message: %s\n", msgId)
		}
	}

	producer2, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
		CompressionType: pulsar.ZSTD,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer2.Close()
	for i := 0; i < 1; i++ {
		if msgId, err := producer2.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("producerCompression type ZSTD functions-demo" ),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published ZSTD CompressionType message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 3; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received CompressionType msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func producerLastSequenceID(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	sequence := int64(10)
	for i := 0; i < 5; i++ {
		if _, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload:    []byte("producerLastSequenceID functions-demo" ),
			SequenceID: &sequence,
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published producerLastSequenceID message: %s\n", producer.LastSequenceID())
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 5; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received producerLastSequenceID msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func producerMessageRouter(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
		MessageRouter: func(msg *pulsar.ProducerMessage, tm pulsar.TopicMetadata) int {
			fmt.Println("Routing message ", msg, " -- Partitions: ", tm.NumPartitions())
			return 2
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 5; i++ {
		if msgID, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("producerMessageRouter functions-demo" ),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published producerMessageRouter message: %s\n", msgID)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 5; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received producerMessageRouter msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func producerDelayMsg(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 2; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload:      []byte("producerDelayMsg functions-demo" ),
			DeliverAfter: 3 * time.Second,
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published Delay message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	ctx, canc := context.WithTimeout(context.Background(), 1*time.Second)
	defer canc()
	msg, err := consumer.Receive(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("1*time.Second Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
	consumer.Ack(msg)

	ctx1, canc1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer canc1()
	msg1, err := consumer.Receive(ctx1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("5*time.Second consumer Received message msgId: %#v -- content: '%s'\n", msg1.ID(), string(msg1.Payload()))
	consumer.Ack(msg1)
}

func producerDelayAbsolute(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 2; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload:   []byte("producerDelayAbsolute functions-demo" ),
			DeliverAt: time.Now().Add(3 * time.Second),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published producerDelayAbsolute message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	ctx, canc := context.WithTimeout(context.Background(), 1*time.Second)
	defer canc()
	msg, err := consumer.Receive(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("1*time.Second Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
	consumer.Ack(msg)

	ctx1, canc1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer canc1()
	msg1, err := consumer.Receive(ctx1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("5*time.Second consumer Received message msgId: %#v -- content: '%s'\n", msg1.ID(), string(msg1.Payload()))
	consumer.Ack(msg1)
}

func consumerShardModel(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerShardModel functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumer Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
	consumer.Close()

	consumer1, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		msg, err := consumer1.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumer1 Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer1.Ack(msg)
	}
	consumer1.Close()
}

func consumerExclusiveModel(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerExclusiveModel functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Exclusive,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 5; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumer Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}

	consumer1, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Exclusive,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer1.Close()
	for i := 0; i < 1; i++ {
		msg, err := consumer1.Receive(context.Background())
		if err != nil {
			fmt.Println(err)
		}
		fmt.Printf("consumer1 Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer1.Ack(msg)
	}

}

func consumerFailoverModel(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerFailoverModel functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Failover,
	})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumer Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
	consumer.Close()

	consumer1, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Failover,
	})
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		msg, err := consumer1.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumer1 Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer1.Ack(msg)
	}
	consumer1.Close()
}

func producerMsg(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("producerMsg functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published message: %s\n", msgId)
		}
		time.Sleep(1 * time.Second)
	}
}

func consumerListener(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	channel := make(chan pulsar.ConsumerMessage, 100)
	options := pulsar.ConsumerOptions{
		Topic:             topic,
		SubscriptionName:  subname,
		Type:              pulsar.Shared,
		ReceiverQueueSize: 1,
		DLQ:               &pulsar.DLQPolicy{MaxDeliveries: 1, Topic: "persistent://251000691/default/sub1-dlq"},
	}

	options.MessageChannel = channel
	consumer, err := client.Subscribe(options)
	if err != nil {
		log.Fatal(err)
	}

	defer consumer.Close()
	for cm := range channel {
		msg := cm.Message
		fmt.Printf("consumerListener Received message  msgId: %v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func consumerReader(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 1; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerReader functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published Reader message: %s\n", msgId)
		}
	}

	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          topic,
		StartMessageID: pulsar.EarliestMessageID(),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()
	for i := 0; i < 1; i++ {
		msg, err := reader.Next(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumerReader Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
	}
}

func consumerReaderOnSpecificMessage(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	msgIDs := [10]pulsar.MessageID{}
	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerReaderOnSpecificMessage functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published Reader message: %s\n", msgId)
			msgIDs[i] = msgId
		}
	}

	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          topic,
		StartMessageID: msgIDs[4],
	})
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()
	for i := 4; i < 10; i++ {
		msg, err := reader.Next(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("consumerReader Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
	}
}

func consumerEarliestMessageID(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 1; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerEarliestMessageID functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published consumerEarliestMessageID message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            subname,
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 1; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received consumerEarliestMessageID msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}

}

func consumerTopics(secretId, secretKey, uin, region, brokerUrl, topic, subname, topic1 string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	ctx := context.Background()
	for i := 0; i < 1; i++ {
		if msgId, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte("consumerTopics functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published topic message: %s\n", msgId)
		}
	}

	producer1, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic1,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer1.Close()
	ctx1 := context.Background()
	for i := 0; i < 1; i++ {
		if msgId, err := producer1.Send(ctx1, &pulsar.ProducerMessage{
			Payload: []byte("Hello Topic1"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published topic1 message: %s\n", msgId)
		}
	}

	topics := []string{topic, topic1}
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topics:           topics,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 2; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func consumerPattern(secretId, secretKey, uin, region, brokerUrl, topic, subname, pattern string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	ctx := context.Background()
	for i := 0; i < 1; i++ {
		if msgId, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte("consumerPattern functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published pattern message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		TopicsPattern:    pattern,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 1; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received Pattern message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func consumerTimeout(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	ctx := context.Background()
	for i := 0; i < 1; i++ {
		if msgId, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte("consumerTimeout functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	ctx1, cancel := context.WithTimeout(context.Background(), 5000*time.Millisecond)
	defer cancel()
	for i := 0; i < 1; i++ {
		msg, err := consumer.Receive(ctx1)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received timeout message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
	}
}

func consumerTag(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 1; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerTag tag-A functions-demo"),
			Tags:    []string{"tag-A"},
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published tag-A message: %s\n", msgId)
		}
	}

	for i := 0; i < 1; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerTag tag-B functions-demo"),
			Tags:    []string{"tag-B"},
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published tag-B message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
		TagMapTopicNames: map[string]string{topic: "tag-A"},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 1; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received tag-A message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		fmt.Printf("Message Properties : %v\n", msg.Properties())
		consumer.Ack(msg)
	}
}

func consumerKey(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	for i := 0; i < 1; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerKey functions-demo"),
			Key:     "tdmq-key-functions-demo",
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published key message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 1; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received key message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		fmt.Printf("Message Properties : %v\n", msg.Properties())
		consumer.Ack(msg)
	}
}

func producerBatchMessages(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	batchSize, numOfMessages := 2, 10
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching:     false,
		BatchingMaxMessages: uint(batchSize),
		Topic:               topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < numOfMessages; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("producerBatchMessages functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published BatchMessages message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < numOfMessages; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received BatchMessages message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func consumerSeekByMessageID(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	var seekID pulsar.MessageID
	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerSeekByMessageID functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published consumerSeekByMessageID message: %s\n", msgId)
			if i == 4 {
				seekID = msgId
			}
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("First Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}

	err = consumer.Seek(seekID)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 6; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("After SeekBymsgId Received message msgId : %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func consumerSeekByTime(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerSeekByTime functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	currentTimestamp := time.Now()
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
	time.Sleep(time.Duration(5) * time.Second)
	err = consumer.SeekByTime(currentTimestamp)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 1; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received seek by time msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func consumerAckID(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	msgIDs := [2]pulsar.MessageID{}
	for i := 0; i < 2; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerAckID functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published message: %s\n", msgId)
			msgIDs[i] = msgId
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 2; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received AckID msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.AckID(msgIDs[i])
		//consumer.Ack(msg)
	}
}

func consumerNack(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 20; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerNack functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published message: %s\n", msgId)
		}
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 20; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		if i%2 == 0 {
			fmt.Printf("Received ACK msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
			consumer.Ack(msg)
		} else {
			fmt.Printf("Received NACK msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
			consumer.Nack(msg)
		}
	}

	for i := 0; i < 20; i += 2 {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("again Received Nack msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func consumerCompressionWithBatches(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching:         true,
		Topic:                   topic,
		CompressionType:         pulsar.LZ4,
		BatchingMaxPublishDelay: 1 * time.Minute,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 10; i++ {
		producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("msg-content-%d", i)),
		}, nil)
		fmt.Printf("Published CompressionWithBatches message\n")
	}

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 0; i < 10; i++ {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Received CompressionWithBatches msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
			consumer.Ack(msg)
		}
	}
}

func consumerDLQTopic(secretId, secretKey, uin, region, brokerUrl, topic, subname, dlqtopic string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	dlqconsumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            dlqtopic,
		SubscriptionName: subname,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer dlqconsumer.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:               topic,
		SubscriptionName:    subname,
		Type:                pulsar.Shared,
		NackRedeliveryDelay: 1 * time.Second,
		DLQ: &pulsar.DLQPolicy{
			MaxDeliveries: 3,
			Topic:         dlqtopic,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerDLQ functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published consumerDLQ message: %s\n", msgId)
		}
	}

	for i := 0; i < 10; i++ {
		msg, _ := consumer.Receive(context.Background())
		if i%2 == 0 {
			consumer.Ack(msg)
			fmt.Printf("consumer Ack msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		} else {
			consumer.Nack(msg)
			fmt.Printf("consumer Nack msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		}
	}

	for i := 0; i < 2; i++ {
		for i := 0; i < 5; i++ {
			msg, _ := consumer.Receive(context.Background())
			consumer.Nack(msg)
			fmt.Printf("consumer Nack msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		}
	}

	for i := 0; i < 5; i++ {
		ctx, canc := context.WithTimeout(context.Background(), 5*time.Second)
		defer canc()
		msg, err := dlqconsumer.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Received consumer DLQ msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
			dlqconsumer.Ack(msg)
		}
	}

}

func consumerRetryTopic(secretId, secretKey, uin, region, brokerUrl, topic, subname, dlqtopic, retrytopic string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	dlqconsumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            dlqtopic,
		SubscriptionName: subname,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer dlqconsumer.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
		DLQ: &pulsar.DLQPolicy{
			MaxDeliveries: 2,
			Topic:         dlqtopic,
			RetryTopic:    retrytopic,
		},
		EnableRetry: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < 10; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("consumerRetryTopic functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published consumerRetryTopic message: %s\n", msgId)
		}
	}

	for i := 0; i < 10; i++ {
		msg, _ := consumer.Receive(context.Background())
		fmt.Printf("ReconsumeLater msgId: %#v -- content: '%s', --topic:'%v'\n", msg.ID(), string(msg.Payload()), msg.Topic())
		//consumer.ReconsumeLater(msg, pulsar.NewReconsumeOptionsWithLevel(1))
		consumer.ReconsumeLater(msg, pulsar.NewReconsumeOptionsWithTime(5, time.Second))
	}

	for i := 0; i < 10; i++ {
		ctx, canc := context.WithTimeout(context.Background(), 5*time.Second)
		defer canc()
		msg, err := consumer.Receive(ctx)
		if err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("after ReconsumeLater Revieve msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
			consumer.Ack(msg)
		}
	}
}

func consumerPartions(secretId, secretKey, uin, region, brokerUrl, topic, subname string) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topics, err := client.TopicPartitions(topic)
	for _, partion := range topics {
		fmt.Println("partion=%s", partion)
	}
}

func vpcNetModel(secretId, secretKey, uin, region, brokerUrl, topic, subname, netModel string, num int) {
	authParams := make(map[string]string)
	authParams["secretId"] = secretId
	authParams["secretKey"] = secretKey
	authParams["ownerUin"] = uin
	authParams["uin"] = uin
	authParams["region"] = region
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:       brokerUrl,
		AuthCloud: pulsar.NewAuthenticationCloudCam(authParams),
		ListenerName:  netModel,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		DisableBatching: true,
		Topic:           topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: subname,
		Type:             pulsar.Shared,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	for i := 0; i < num; i++ {
		if msgId, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(" functions-demo"),
		}); err != nil {
			log.Fatal(err)
		} else {
			fmt.Printf("Published message: %s\n", msgId)
		}

		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Received message msgId: %#v -- content: '%s'\n", msg.ID(), string(msg.Payload()))
		consumer.Ack(msg)
	}
}

func main() {
	secretId := os.Args[1]
	secretKey := os.Args[2]
	uin := os.Args[3]
	region := os.Args[4]
	brokerUrl := os.Args[5]
	fmt.Printf("brokerUrl = %s \n", brokerUrl)
	casesence := os.Args[6]
	fmt.Printf("casesence = %s \n", casesence)
	topic := os.Args[7]
	subname := os.Args[8]
	if casesence == "produceAndConsumerDemo" {
		num, _ := strconv.Atoi(os.Args[9])
		produceAndConsumerDemo(secretId, secretKey, uin, region, brokerUrl, topic, subname, num)
	} else if casesence == "consumerShardModel" {
		consumerShardModel(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerGlobalOrder" {
		consumerShardModel(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerLocalOrder" {
		consumerShardModel(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerExclusiveModel" {
		consumerExclusiveModel(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerFailoverModel" {
		consumerFailoverModel(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerListener" {
		consumerListener(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerReader" {
		consumerReader(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerReaderOnSpecificMessage" {
		consumerReaderOnSpecificMessage(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerEarliestMessageID" {
		consumerEarliestMessageID(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerTopics" {
		var topic1 string = os.Args[9]
		consumerTopics(secretId, secretKey, uin, region, brokerUrl, topic, subname, topic1)
	} else if casesence == "consumerPattern" {
		var pattern string = os.Args[9]
		consumerPattern(secretId, secretKey, uin, region, brokerUrl, topic, subname, pattern)
	} else if casesence == "consumerTimeout" {
		consumerTimeout(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerTag" {
		consumerTag(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerKey" {
		consumerKey(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "producerDelayMsg" {
		producerDelayMsg(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "producerBatchMessages" {
		producerBatchMessages(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "producerAsync" {
		producerAsync(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "producerCompression" {
		producerCompression(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "producerLastSequenceID" {
		producerLastSequenceID(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "producerMessageRouter" {
		producerMessageRouter(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "producerDelayAbsolute" {
		producerDelayAbsolute(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerSeekByMessageID" {
		consumerSeekByMessageID(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerSeekByTime" {
		consumerSeekByTime(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerAckID" {
		consumerAckID(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerNack" {
		consumerNack(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerPartions" {
		consumerPartions(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerCompressionWithBatches" {
		consumerCompressionWithBatches(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	} else if casesence == "consumerDLQTopic" {
		var dlqtopic string = os.Args[9]
		consumerDLQTopic(secretId, secretKey, uin, region, brokerUrl, topic, subname, dlqtopic)
	} else if casesence == "consumerRetryTopic" {
		var dlqtopic string = os.Args[9]
		var retrytopic string = os.Args[10]
		consumerRetryTopic(secretId, secretKey, uin, region, brokerUrl, topic, subname, dlqtopic, retrytopic)
	} else if casesence == "vpcNetModel" {
		var netModel string = os.Args[9]
		num, _ := strconv.Atoi(os.Args[10])
		vpcNetModel(secretId, secretKey, uin, region, brokerUrl, topic, subname, netModel, num)
	} else {
		producerMsg(secretId, secretKey, uin, region, brokerUrl, topic, subname)
	}

}
