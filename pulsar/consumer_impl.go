// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package pulsar

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/TencentCloud/tdmq-go-client/pulsar/internal"

	pb "github.com/TencentCloud/tdmq-go-client/pulsar/internal/pulsar_proto"
	"github.com/TencentCloud/tdmq-go-client/pulsar/log"
)

var (
	consumersOpened = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsar_client_consumers_opened",
		Help: "Counter of consumers created by the client",
	})

	consumersClosed = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsar_client_consumers_closed",
		Help: "Counter of consumers closed by the client",
	})

	consumersPartitions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulsar_client_consumers_partitions_active",
		Help: "Counter of individual partitions the consumers are currently active",
	})
)

var ErrConsumerClosed = errors.New("consumer closed")

const defaultNackRedeliveryDelay = 1 * time.Minute

type acker interface {
	AckID(id trackingMessageID)
	NackID(id trackingMessageID)
}

type consumer struct {
	sync.Mutex
	topic                     string
	client                    *client
	options                   ConsumerOptions
	consumers                 []*partitionConsumer
	consumerName              string
	disableForceTopicCreation bool

	// channel used to deliver message to clients
	messageCh chan ConsumerMessage

	dlq       *dlqRouter
	closeOnce sync.Once
	closeCh   chan struct{}
	errorCh   chan error
	ticker    *time.Ticker

	log log.Logger
}

func newConsumer(client *client, options ConsumerOptions) (Consumer, error) {
	if options.Topic == "" && options.Topics == nil && options.TopicsPattern == "" {
		return nil, newError(TopicNotFound, "topic is required")
	}

	if options.SubscriptionName == "" {
		return nil, newError(SubscriptionNotFound, "subscription name is required for consumer")
	}

	if options.ReceiverQueueSize <= 0 {
		options.ReceiverQueueSize = defaultReceiverQueueSize
	}

	if options.Interceptors == nil {
		options.Interceptors = defaultConsumerInterceptors
	}

	if options.Name == "" {
		options.Name = generateRandomName()
	}

	if options.Schema != nil && options.Schema.GetSchemaInfo() != nil {
		if options.Schema.GetSchemaInfo().Type == NONE {
			options.Schema = NewBytesSchema(nil)
		}
	}

	// did the user pass in a message channel?
	messageCh := options.MessageChannel
	if options.MessageChannel == nil {
		messageCh = make(chan ConsumerMessage, 10)
	}

	//For Tencent TDMQ retry
	if options.EnableRetry && (options.Topic != "" || len(options.Topics) > 0) {
		var topicFirst string
		if options.Topic != "" {
			topicFirst = options.Topic
			options.Topics = append(options.Topics, options.Topic)
			options.Topic = ""
		} else {
			topicFirst = options.Topics[0]
		}
		topicFirstName, err := internal.ParseTopicName(topicFirst)
		if err != nil {
			return nil, err
		}
		retryLetterTopic := topicFirstName.Namespace + "/" + options.SubscriptionName + "-retry"
		deadLetterTopic := topicFirstName.Namespace + "/" + options.SubscriptionName + "-dlq"
		if options.DLQ == nil {
			options.DLQ = &DLQPolicy{
				MaxDeliveries: 16,
				RetryTopic:    retryLetterTopic,
				Topic:         deadLetterTopic,
			}
		} else {
			if options.DLQ.RetryTopic == "" {
				options.DLQ.RetryTopic = retryLetterTopic
			}
			if options.DLQ.Topic == "" {
				options.DLQ.Topic = deadLetterTopic
			}
		}
		options.Topics = append(options.Topics, options.DLQ.RetryTopic)
		if options.DelayLevelUtil == nil {
			options.DelayLevelUtil = NewDelayLevelUtil(DefaultMessageDelayLevel)
		}
	}

	dlq, err := newDlqRouter(client, options.DLQ, client.log)
	if err != nil {
		return nil, err
	}

	// normalize as FQDN topics
	var tns []*internal.TopicName
	// single topic consumer
	if options.Topic != "" || len(options.Topics) == 1 {
		topic := options.Topic
		if topic == "" {
			topic = options.Topics[0]
		}

		if tns, err = validateTopicNames(topic); err != nil {
			return nil, err
		}
		topic = tns[0].Name
		return topicSubscribe(client, options, topic, messageCh, dlq)
	}

	if len(options.Topics) > 1 {
		if tns, err = validateTopicNames(options.Topics...); err != nil {
			return nil, err
		}
		for i := range options.Topics {
			options.Topics[i] = tns[i].Name
		}

		return newMultiTopicConsumer(client, options, options.Topics, messageCh, dlq)
	}

	//TODO DisableRetry should be set as true of false in TopicsPattern?
	if options.TopicsPattern != "" {
		tn, err := internal.ParseTopicName(options.TopicsPattern)
		if err != nil {
			return nil, err
		}

		pattern, err := extractTopicPattern(tn)
		if err != nil {
			return nil, err
		}
		return newRegexConsumer(client, options, tn, pattern, messageCh, dlq)
	}

	return nil, newError(ResultInvalidTopicName, "topic name is required for consumer")
}

func newInternalConsumer(client *client, options ConsumerOptions, topic string,
	messageCh chan ConsumerMessage, dlq *dlqRouter, disableForceTopicCreation bool) (*consumer, error) {

	consumer := &consumer{
		topic:                     topic,
		client:                    client,
		options:                   options,
		disableForceTopicCreation: disableForceTopicCreation,
		messageCh:                 messageCh,
		closeCh:                   make(chan struct{}),
		errorCh:                   make(chan error),
		dlq:                       dlq,
		log:                       client.log.SubLogger(log.Fields{"topic": topic}),
		consumerName:              options.Name,
	}

	err := consumer.internalTopicSubscribeToPartitions()
	if err != nil {
		return nil, err
	}

	// set up timer to monitor for new partitions being added
	duration := options.AutoDiscoveryPeriod
	if duration <= 0 {
		duration = defaultAutoDiscoveryDuration
	}
	consumer.ticker = time.NewTicker(duration)

	go func() {
		for {
			select {
			case <-consumer.closeCh:
				return
			case <-consumer.ticker.C:
				consumer.log.Debug("Auto discovering new partitions")
				consumer.internalTopicSubscribeToPartitions()
			}
		}
	}()

	return consumer, nil
}

// Name returns the name of consumer.
func (c *consumer) Name() string {
	return c.consumerName
}

func (c *consumer) internalTopicSubscribeToPartitions() error {
	partitions, err := c.client.TopicPartitions(c.topic)
	if err != nil {
		return err
	}

	oldNumPartitions := 0
	newNumPartitions := len(partitions)

	c.Lock()
	defer c.Unlock()
	oldConsumers := c.consumers

	if oldConsumers != nil {
		oldNumPartitions = len(oldConsumers)
		if oldNumPartitions == newNumPartitions {
			c.log.Debug("Number of partitions in topic has not changed")
			return nil
		}

		c.log.WithField("old_partitions", oldNumPartitions).
			WithField("new_partitions", newNumPartitions).
			Info("Changed number of partitions in topic")
	}

	c.consumers = make([]*partitionConsumer, newNumPartitions)

	// Copy over the existing consumer instances
	for i := 0; i < oldNumPartitions; i++ {
		c.consumers[i] = oldConsumers[i]
	}

	type ConsumerError struct {
		err       error
		partition int
		consumer  *partitionConsumer
	}

	receiverQueueSize := c.options.ReceiverQueueSize
	metadata := c.options.Properties

	//For Tencent TDMQ tag
	tagMapTopicNames := make(map[string]string)
	if c.options.TagMapTopicNames != nil {
		for topic, tag := range c.options.TagMapTopicNames {
			if internal.ParseTopicNameToString(c.topic) == internal.ParseTopicNameToString(topic) {
				tagMapTopicNames[internal.ParseTopicNameToString(topic)] = tag
			}
		}
	}
	tagPatternMapTopicNames := make(map[string]string)
	if c.options.TagPatternMapTopicNames != nil {
		for topic, tagPattern := range c.options.TagPatternMapTopicNames {
			if internal.ParseTopicNameToString(c.topic) == internal.ParseTopicNameToString(topic) {
				tagPatternMapTopicNames[internal.ParseTopicNameToString(topic)] = tagPattern
			}
		}
	}

	partitionsToAdd := newNumPartitions - oldNumPartitions
	var wg sync.WaitGroup
	ch := make(chan ConsumerError, partitionsToAdd)
	wg.Add(partitionsToAdd)

	for partitionIdx := oldNumPartitions; partitionIdx < newNumPartitions; partitionIdx++ {
		partitionTopic := partitions[partitionIdx]

		go func(idx int, pt string) {
			defer wg.Done()

			var nackRedeliveryDelay time.Duration
			if c.options.NackRedeliveryDelay == 0 {
				nackRedeliveryDelay = defaultNackRedeliveryDelay
			} else {
				nackRedeliveryDelay = c.options.NackRedeliveryDelay
			}
			opts := &partitionConsumerOpts{
				topic:                      pt,
				consumerName:               c.consumerName,
				subscription:               c.options.SubscriptionName,
				subscriptionType:           c.options.Type,
				subscriptionInitPos:        c.options.SubscriptionInitialPosition,
				partitionIdx:               idx,
				receiverQueueSize:          receiverQueueSize,
				nackRedeliveryDelay:        nackRedeliveryDelay,
				metadata:                   metadata,
				replicateSubscriptionState: c.options.ReplicateSubscriptionState,
				startMessageID:             trackingMessageID{},
				subscriptionMode:           durable,
				readCompacted:              c.options.ReadCompacted,

				//For Tencent TDMQ tag
				tagMapTopicNames:        tagMapTopicNames,
				tagPatternMapTopicNames: tagPatternMapTopicNames,

				delayLevelUtil: c.options.DelayLevelUtil,

				interceptors:         c.options.Interceptors,
				maxReconnectToBroker: c.options.MaxReconnectToBroker,
				keySharedPolicy:      c.options.KeySharedPolicy,
				schema:               c.options.Schema,
			}
			cons, err := newPartitionConsumer(c, c.client, opts, c.messageCh, c.dlq)
			ch <- ConsumerError{
				err:       err,
				partition: idx,
				consumer:  cons,
			}
		}(partitionIdx, partitionTopic)
	}

	go func() {
		wg.Wait()
		close(ch)
	}()

	for ce := range ch {
		if ce.err != nil {
			err = ce.err
		} else {
			c.consumers[ce.partition] = ce.consumer
		}
	}

	if err != nil {
		// Since there were some failures,
		// cleanup all the partitions that succeeded in creating the consumer
		for _, c := range c.consumers {
			if c != nil {
				c.Close()
			}
		}
		return err
	}

	consumersPartitions.Add(float64(partitionsToAdd))
	return nil
}

func topicSubscribe(client *client, options ConsumerOptions, topic string,
	messageCh chan ConsumerMessage, dlqRouter *dlqRouter) (Consumer, error) {
	c, err := newInternalConsumer(client, options, topic, messageCh, dlqRouter, false)
	if err == nil {
		consumersOpened.Inc()
	}
	return c, err
}

func (c *consumer) Subscription() string {
	return c.options.SubscriptionName
}

func (c *consumer) Unsubscribe() error {
	c.Lock()
	defer c.Unlock()

	var errMsg string
	for _, consumer := range c.consumers {
		if err := consumer.Unsubscribe(); err != nil {
			errMsg += fmt.Sprintf("topic %s, subscription %s: %s", consumer.topic, c.Subscription(), err)
		}
	}
	if errMsg != "" {
		return fmt.Errorf(errMsg)
	}
	return nil
}

func (c *consumer) Receive(ctx context.Context) (message Message, err error) {
	for {
		select {
		case <-c.closeCh:
			return nil, ErrConsumerClosed
		case cm, ok := <-c.messageCh:
			if !ok {
				return nil, ErrConsumerClosed
			}
			return cm.Message, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *consumer) ReconsumeLater(message Message, reconsumeOptions ReconsumeOptions) error {
	if !c.options.EnableRetry {
		return errors.New("This Consumer config retry disabled. ")
	}
	topicName, err := internal.ParseTopicName(message.Topic())
	if err != nil {
		return err
	}
	index := 0
	if topicName.Partition >= 0 {
		index = topicName.Partition
	}
	wg := sync.WaitGroup{}
	wg.Add(1)
	prod, prodMsg, desType, err := c.consumers[index].internalBeforeReconsume(message, reconsumeOptions)
	if err != nil {
		return err
	}

	c.consumers[index].internalReconsumeAsync(prod, message, prodMsg, desType, func(id MessageID, producerMessage *ProducerMessage, e error) {
		err = e
		wg.Done()
	})
	wg.Wait()
	return err
}

func (c *consumer) ReconsumeLaterAsync(message Message, reconsumeOptions ReconsumeOptions, callback func(MessageID, *ProducerMessage, error)) {
	if !c.options.EnableRetry {
		c.log.Error("This Consumer config retry disabled. ")
		return
	}
	topicName, err := internal.ParseTopicName(message.Topic())
	if err != nil {
		c.log.Error(err)
		return
	}
	index := 0
	if topicName.Partition >= 0 {
		index = topicName.Partition
	}
	prod, prodMsg, desType, err := c.consumers[index].internalBeforeReconsume(message, reconsumeOptions)
	if err != nil {
		c.log.Error(err)
		return
	}
	c.consumers[index].internalReconsumeAsync(prod, message, prodMsg, desType, callback)
}

// Messages
func (c *consumer) Chan() <-chan ConsumerMessage {
	return c.messageCh
}

// Ack the consumption of a single message
func (c *consumer) Ack(msg Message) {
	c.AckID(msg.ID())
}

// Ack the consumption of a single message, identified by its MessageID
func (c *consumer) AckID(msgID MessageID) {
	mid, ok := c.messageID(msgID)
	if !ok {
		return
	}

	if mid.consumer != nil {
		mid.Ack()
		return
	}

	c.consumers[mid.partitionIdx].AckID(mid)
}

func (c *consumer) Nack(msg Message) {
	c.NackID(msg.ID())
}

func (c *consumer) NackID(msgID MessageID) {
	mid, ok := c.messageID(msgID)
	if !ok {
		return
	}

	if mid.consumer != nil {
		mid.Nack()
		return
	}

	c.consumers[mid.partitionIdx].NackID(mid)
}

func (c *consumer) Close() {
	c.closeOnce.Do(func() {
		c.Lock()
		defer c.Unlock()

		var wg sync.WaitGroup
		for i := range c.consumers {
			wg.Add(1)
			go func(pc *partitionConsumer) {
				defer wg.Done()
				pc.Close()
			}(c.consumers[i])
		}
		wg.Wait()
		close(c.closeCh)
		c.ticker.Stop()
		c.client.handlers.Del(c)
		c.dlq.close()
		consumersClosed.Inc()
		consumersPartitions.Sub(float64(len(c.consumers)))
	})
}

func (c *consumer) Seek(msgID MessageID) error {
	c.Lock()
	defer c.Unlock()

	if len(c.consumers) > 1 {
		return errors.New("for partition topic, seek command should perform on the individual partitions")
	}

	mid, ok := c.messageID(msgID)
	if !ok {
		return nil
	}

	return c.consumers[mid.partitionIdx].Seek(mid)
}

func (c *consumer) SeekByTime(time time.Time) error {
	c.Lock()
	defer c.Unlock()
	if len(c.consumers) > 1 {
		return errors.New("for partition topic, seek command should perform on the individual partitions")
	}

	return c.consumers[0].SeekByTime(time)
}

var r = &random{
	R: rand.New(rand.NewSource(time.Now().UnixNano())),
}

type random struct {
	sync.Mutex
	R *rand.Rand
}

func generateRandomName() string {
	r.Lock()
	defer r.Unlock()
	chars := "abcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, 5)
	for i := range bytes {
		bytes[i] = chars[r.R.Intn(len(chars))]
	}
	return string(bytes)
}

func toProtoSubType(st SubscriptionType) pb.CommandSubscribe_SubType {
	switch st {
	case Exclusive:
		return pb.CommandSubscribe_Exclusive
	case Shared:
		return pb.CommandSubscribe_Shared
	case Failover:
		return pb.CommandSubscribe_Failover
	case KeyShared:
		return pb.CommandSubscribe_Key_Shared
	}

	return pb.CommandSubscribe_Exclusive
}

func toProtoInitialPosition(p SubscriptionInitialPosition) pb.CommandSubscribe_InitialPosition {
	switch p {
	case SubscriptionPositionLatest:
		return pb.CommandSubscribe_Latest
	case SubscriptionPositionEarliest:
		return pb.CommandSubscribe_Earliest
	}

	return pb.CommandSubscribe_Latest
}

func (c *consumer) messageID(msgID MessageID) (trackingMessageID, bool) {
	mid, ok := toTrackingMessageID(msgID)
	if !ok {
		c.log.Warnf("invalid message id type %T", msgID)
		return trackingMessageID{}, false
	}

	partition := int(mid.partitionIdx)
	// did we receive a valid partition index?
	if partition < 0 || partition >= len(c.consumers) {
		c.log.Warnf("invalid partition index %d expected a partition between [0-%d]",
			partition, len(c.consumers))
		return trackingMessageID{}, false
	}

	return mid, true
}
