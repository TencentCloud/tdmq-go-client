<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Tencent TDMQ Go Client Library

A Go client library for the [Tencent TDMQ](https://cloud.tencent.com/product/tdmq) project.

## Goal

This projects is developing a pure-Go client library for TDMQ that does not
depend on the C++ Pulsar library.

Once feature parity and stability are reached, this will supersede the current
CGo based library.

## Requirements

- Go 1.11+

## Status

Check the Projects page at https://github.com/TencentCloud/tdmq-go-client/projects for
tracking the status and the progress.

## Usage

Import the client library:

```go
import "github.com/TencentCloud/tdmq-go-client/pulsar"
```

Create a Producer:

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})

defer client.Close()

producer, err := client.CreateProducer(pulsar.ProducerOptions{
	Topic: "my-topic",
})

_, err = producer.Send(context.Background(), &pulsar.ProducerMessage{
	Payload: []byte("hello"),
})

defer producer.Close()

if err != nil {
    fmt.Println("Failed to publish message", err)
}
fmt.Println("Published message")
```

Create a Consumer:

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{
    URL: "pulsar://localhost:6650",
})

defer client.Close()

consumer, err := client.Subscribe(pulsar.ConsumerOptions{
        Topic:            "my-topic",
        SubscriptionName: "my-sub",
        Type:             pulsar.Shared,
    })

defer consumer.Close()

msg, err := consumer.Receive(context.Background())
    if err != nil {
        log.Fatal(err)
    }

fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
            msg.ID(), string(msg.Payload()))

```

Create a Reader:

```go
client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://localhost:6650"})
if err != nil {
	log.Fatal(err)
}

defer client.Close()

reader, err := client.CreateReader(pulsar.ReaderOptions{
	Topic:          "topic-1",
	StartMessageID: pulsar.EarliestMessageID(),
})
if err != nil {
	log.Fatal(err)
}
defer reader.Close()

for reader.HasNext() {
	msg, err := reader.Next(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
		msg.ID(), string(msg.Payload()))
}
```
Auth Cloud Config:
```go
authParams := make(map[string]string)
authParams["secretId"] = "AKxxxxxxxxxxCx"
authParams["secretKey"] = "SDxxxxxxxxxxCb"
authParams["region"] = "ap-guangzhou"
authParams["ownerUin"] = "xxxxxxxxxx"
authParams["uin"] = "xxxxxxxxxx"
client, err := pulsar.NewClient(pulsar.ClientOptions{
	URL:       "pulsar://9.xx.xx.8:6650",
	AuthCloud: pulsar.NewAuthenticationCloudCam(authParams), //在这里配置CAM认证
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()
```

## More

More information please see [TDMQ GO Client Doc](https://cloud.tencent.com/document/product/1179/44831)

## License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
