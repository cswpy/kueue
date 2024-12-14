package kueue

import (
	"context"
	"fmt"
	"kueue/kueue/proto"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func generateMessages(n int) []*proto.ProducerMessage {
	msgs := make([]*proto.ProducerMessage, n)
	for i := 0; i < n; i++ {
		msgs[i] = &proto.ProducerMessage{
			Key:   fmt.Sprintf("key:%d", i),
			Value: fmt.Sprintf("value:%d", i),
		}
	}
	return msgs
}

func consumerToProducerMessages(msgs []*proto.ConsumerMessage) []*proto.ProducerMessage {
	newMsgs := make([]*proto.ProducerMessage, len(msgs))
	for i, msg := range msgs {
		newMsgs[i] = &proto.ProducerMessage{
			Key:   msg.Key,
			Value: msg.Value,
		}
	}
	return newMsgs
}

func checkMessageContent(t *testing.T, msgs1 []*proto.ProducerMessage, msgs2 []*proto.ConsumerMessage) {
	assert.Len(t, msgs1, len(msgs2))
	msgs3 := consumerToProducerMessages(msgs2)
	assert.ElementsMatch(t, msgs1, msgs3)
}

// Check that the consumes messages have correct offsets and monotonic timestamps
func checkMessageMetadata(t *testing.T, msgs []*proto.ConsumerMessage, baseOffset int) {
	assert.EqualValues(t, msgs[0].Offset, baseOffset)
	for i := 1; i < len(msgs); i++ {
		assert.EqualValues(t, msgs[i].Offset, msgs[i-1].Offset+1)
		assert.GreaterOrEqual(t, msgs[i].Timestamp, msgs[i-1].Timestamp)
	}
}

func TestBrokerProduceConsume(t *testing.T) {

	b := NewMockBroker(*logrus.WithField("test", "broker"))

	msgs := generateMessages(5)

	produceRequest := proto.ProduceRequest{
		TopicName:   "topic1",
		ProducerId:  "producer1",
		PartitionId: 0,
		Messages:    msgs,
	}

	resp1, err := b.Produce(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	consumeRequest := proto.ConsumeRequest{
		TopicName:   "topic1",
		PartitionId: 0,
		ConsumerId:  "consumer1",
	}

	resp2, err := b.Consume(context.Background(), &consumeRequest)
	assert.NoError(t, err)
	assert.Equal(t, resp2.TopicName, "topic1")
	assert.Len(t, resp2.Records, len(msgs))
	checkMessageContent(t, msgs, resp2.Records)
	checkMessageMetadata(t, resp2.Records, int(resp1.BaseOffset))
}

func TestBrokerConsumeEmptyPartition(t *testing.T) {
	b := NewMockBroker(*logrus.WithField("test", "broker"))

	msgs := generateMessages(1)

	produceRequest := proto.ProduceRequest{
		TopicName:   "topic1",
		ProducerId:  "producer1",
		PartitionId: 0,
	}

	b.Produce(context.Background(), &produceRequest)

	// Try consuming from an empty partition
	consumeRequest := proto.ConsumeRequest{
		TopicName:   "topic1",
		PartitionId: 0,
		ConsumerId:  "consumer1",
	}

	resp, err := b.Consume(context.Background(), &consumeRequest)

	assert.Error(t, err)
	assert.Nil(t, resp)

	produceRequest.Messages = msgs
	b.Produce(context.Background(), &produceRequest)

	resp, err = b.Consume(context.Background(), &consumeRequest)
	assert.NoError(t, err)
	assert.Equal(t, resp.TopicName, "topic1")
	checkMessageContent(t, msgs, resp.Records)

	resp, err = b.Consume(context.Background(), &consumeRequest)
	assert.Equal(t, resp.TopicName, "topic1")
	assert.Empty(t, resp.Records)
	assert.NoError(t, err)

}

func TestBrokerMPSC(t *testing.T) {
	var (
		numProducer           = 2
		numMessagePerProducer = 10
		numMessagePerRequest  = 5
	)
	numBatch := numMessagePerProducer / numMessagePerRequest
	b := NewMockBroker(*logrus.WithField("test", "broker"))
	msgs := generateMessages(numProducer * numMessagePerProducer)

	var wg sync.WaitGroup

	for i := 0; i < numProducer; i++ {
		wg.Add(1)
		go func(i int, msgs []*proto.ProducerMessage) {
			producerId := fmt.Sprintf("producer-%d", i)
			for batchIdx := 0; batchIdx < numBatch; batchIdx++ {
				begin := batchIdx * numMessagePerRequest
				end := begin + numMessagePerRequest
				produceRequest := proto.ProduceRequest{
					TopicName:   "topic1",
					ProducerId:  producerId,
					PartitionId: 0,
					Messages:    msgs[begin:end],
				}
				_, err := b.Produce(context.Background(), &produceRequest)
				assert.NoError(t, err)
			}
			wg.Done()
		}(i, msgs[i*numMessagePerProducer:(i+1)*numMessagePerProducer])
	}

	consumeRequest := proto.ConsumeRequest{
		TopicName:   "topic1",
		PartitionId: 0,
		ConsumerId:  "consumer-1",
	}

	done := make(chan struct{})

	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait a bit for at least one message to be produced
	time.Sleep(100 * time.Millisecond)

	var resp1 *proto.ConsumeResponse
	var err error
	var arr []*proto.ConsumerMessage

	select {
	case <-done:
		resp1, err = b.Consume(context.Background(), &consumeRequest)
		assert.NoError(t, err)
		arr = append(arr, resp1.Records...)
		return
	default:
		resp1, err = b.Consume(context.Background(), &consumeRequest)
		assert.NoError(t, err)
		arr = append(arr, resp1.Records...)
	}

	checkMessageContent(t, msgs, arr)
	checkMessageMetadata(t, arr, 0)
}
