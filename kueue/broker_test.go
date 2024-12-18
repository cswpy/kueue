package kueue

import (
	"context"
	"time"

	"encoding/binary"
	"fmt"
	"io"
	"kueue/kueue/proto"
	"os"

	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	// "time"

	// "github.com/puzpuzpuz/xsync"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	proto1 "google.golang.org/protobuf/proto"
)

func readOffsetFile(t *testing.T, brokerName, topicName string, partitionId int32, consumerId string) int32 {
	dirPath := filepath.Join(brokerName, fmt.Sprintf("%s-%d-offset", topicName, partitionId))
	fileName := fmt.Sprintf("%s.bin", consumerId)
	filePath := filepath.Join(dirPath, fileName)

	f, err := os.Open(filePath)
	if os.IsNotExist(err) {
		// If the file does not exist, return 0 as the default offset
		return 0
	}
	assert.NoError(t, err, "Should be able to open offset file for consumer %s", consumerId)
	defer f.Close()

	var storedOffset int32
	err = binary.Read(f, binary.LittleEndian, &storedOffset)
	assert.NoError(t, err, "Should read offset for consumer %s", consumerId)
	return storedOffset
}

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

// Function to verify messages persisted to disk
func verifyMessagesPersistedToDisk(t *testing.T, brokerName, topicName string, partitionId int32, expectedMsgs []*proto.ConsumerMessage) {
	dirPath := filepath.Join(brokerName, fmt.Sprintf("%s-%d", topicName, partitionId))
	files, err := os.ReadDir(dirPath)
	assert.NoError(t, err, "Failed to read directory for persisted messages")
	assert.NotEmpty(t, files, "Expected some files to be created for persisted messages")

	var allMessages []*proto.ConsumerMessage

	for _, fileEntry := range files {
		filePath := filepath.Join(dirPath, fileEntry.Name())
		f, err := os.Open(filePath)
		assert.NoError(t, err)
		defer f.Close()

		for {
			var length uint32
			err := binary.Read(f, binary.LittleEndian, &length)
			if err == io.EOF {
				break
			}
			assert.NoError(t, err, "Failed to read message length")

			data := make([]byte, length)
			_, err = f.Read(data)
			assert.NoError(t, err, "Failed to read message data")

			// Unmarshal the data into a ConsumerMessage
			msg := &proto.ConsumerMessage{}
			err = proto1.Unmarshal(data, msg)
			assert.NoError(t, err, "Failed to unmarshal message")

			allMessages = append(allMessages, msg)
		}
	}

	// Ensure all messages were read
	assert.Equal(t, len(expectedMsgs), len(allMessages), "Not all messages were read from disk")

	// Verify the content of the messages
	for i, msg := range expectedMsgs {
		assert.Equal(t, msg.Key, allMessages[i].Key, "Message keys should match")
		assert.Equal(t, msg.Value, allMessages[i].Value, "Message values should match")
	}
}

func convertToConsumerMessages(producerMessages []*proto.ProducerMessage) []*proto.ConsumerMessage {
	consumerMessages := make([]*proto.ConsumerMessage, len(producerMessages))
	for i, pm := range producerMessages {
		consumerMessages[i] = &proto.ConsumerMessage{
			Key:   pm.Key,
			Value: pm.Value,
			// Set other fields as necessary, e.g., Offset, Timestamp
		}
	}
	return consumerMessages
}

func TestBrokerProduceConsume(t *testing.T) {
	persistBatch := 2
	brokerName := "BK1"
	b := NewMockBroker(*logrus.WithField("test", "broker"), brokerName, persistBatch)

	msgs := generateMessages(5)

	produceRequest := proto.ProduceRequest{
		TopicName:   "topic1",
		ProducerId:  "producer1",
		PartitionId: 0,
		Messages:    msgs,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	// Producing to a broker that is not leader should fail
	_, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.Error(t, err)

	// Appoint broker as leader
	resp2, err := b.AppointAsLeader(context.Background(), &apptRequest)
	assert.NoError(t, err)
	assert.True(t, resp2.LeaderAppointed)

	// Now produce should succeed
	resp1, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	consumeRequest := proto.ConsumeRequest{
		TopicName:   "topic1",
		PartitionId: 0,
		ConsumerId:  "consumer1",
	}

	resp3, err := b.ConsumeMessage(context.Background(), &consumeRequest)
	assert.NoError(t, err)
	assert.Equal(t, resp3.TopicName, "topic1")
	assert.Len(t, resp3.Records, len(msgs))
	checkMessageContent(t, msgs, resp3.Records)
	checkMessageMetadata(t, resp3.Records, int(resp1.BaseOffset))

	// Cleanup
	err = os.RemoveAll(b.BrokerInfo.BrokerName)
	assert.NoError(t, err)
}

func TestBrokerConsumeEmptyPartition(t *testing.T) {
	persistBatch := 2
	brokerName := "BK1"
	b := NewMockBroker(*logrus.WithField("test", "broker"), brokerName, persistBatch)

	msgs := generateMessages(1)

	produceRequest := proto.ProduceRequest{
		TopicName:   "topic1",
		ProducerId:  "producer1",
		PartitionId: 0,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	b.AppointAsLeader(context.Background(), &apptRequest)

	b.ProduceMessage(context.Background(), &produceRequest)

	// Try consuming from an empty partition
	consumeRequest := proto.ConsumeRequest{
		TopicName:   "topic1",
		PartitionId: 0,
		ConsumerId:  "consumer1",
	}

	resp, err := b.ConsumeMessage(context.Background(), &consumeRequest)

	assert.Error(t, err)
	assert.Nil(t, resp)

	produceRequest.Messages = msgs
	b.ProduceMessage(context.Background(), &produceRequest)

	resp, err = b.ConsumeMessage(context.Background(), &consumeRequest)
	assert.NoError(t, err)
	assert.Equal(t, resp.TopicName, "topic1")
	checkMessageContent(t, msgs, resp.Records)

	resp, err = b.ConsumeMessage(context.Background(), &consumeRequest)
	assert.Equal(t, resp.TopicName, "topic1")
	assert.Empty(t, resp.Records)
	assert.NoError(t, err)

	// Cleanup
	err = os.RemoveAll(b.BrokerInfo.BrokerName)
	assert.NoError(t, err)

}

func TestBrokerMPSC(t *testing.T) {
	var (
		numProducer           = 2
		numMessagePerProducer = 10
		numMessagePerRequest  = 5
		persistBatch          = 2
	)
	numBatch := numMessagePerProducer / numMessagePerRequest
	brokerName := "BK1"
	b := NewMockBroker(*logrus.WithField("test", "broker"), brokerName, persistBatch)

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	_, err := b.AppointAsLeader(context.Background(), &apptRequest)

	assert.NoError(t, err)

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
				_, err := b.ProduceMessage(context.Background(), &produceRequest)
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
	var arr []*proto.ConsumerMessage

	select {
	case <-done:
		resp1, err = b.ConsumeMessage(context.Background(), &consumeRequest)
		assert.NoError(t, err)
		arr = append(arr, resp1.Records...)
		// Cleanup
		err = os.RemoveAll(b.BrokerInfo.BrokerName)
		assert.NoError(t, err)
		return
	default:
		resp1, err = b.ConsumeMessage(context.Background(), &consumeRequest)
		assert.NoError(t, err)
		arr = append(arr, resp1.Records...)
		// Cleanup
		err = os.RemoveAll(b.BrokerInfo.BrokerName)
		assert.NoError(t, err)
	}

	checkMessageContent(t, msgs, arr)
	checkMessageMetadata(t, arr, 0)

}

func TestBrokerProducePersist(t *testing.T) {
	persistBatch := 2
	brokerName := "BK1"
	b := NewMockBroker(*logrus.WithField("test", "broker"), brokerName, persistBatch)

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	b.AppointAsLeader(context.Background(), &apptRequest)

	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
		{Key: "key4", Value: "value4"},
		{Key: "key5", Value: "value5"},
	}

	produceRequest := proto.ProduceRequest{
		TopicName:   "topic1",
		ProducerId:  "producer1",
		PartitionId: 0,
		Messages:    msgs,
	}

	// Call Produce
	resp1, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	// Verify that files are created

	dirPath := filepath.Join(b.BrokerInfo.BrokerName, "topic1-0")
	files, err := os.ReadDir(dirPath)
	assert.NoError(t, err)
	assert.NotEmpty(t, files, "No files created in the directory")

	// Read and verify each message
	messageIndex := 0
	for _, file := range files {
		filePath := filepath.Join(dirPath, file.Name())
		f, err := os.Open(filePath)
		assert.NoError(t, err)
		defer f.Close()

		for {
			var length uint32
			err := binary.Read(f, binary.LittleEndian, &length)
			if err == io.EOF {
				break
			}
			assert.NoError(t, err, "Failed to read message length")

			data := make([]byte, length)
			_, err = f.Read(data)
			assert.NoError(t, err, "Failed to read message data")

			// Unmarshal the data into a ConsumerMessage
			msg := &proto.ConsumerMessage{}
			err = proto1.Unmarshal(data, msg)
			assert.NoError(t, err, "Failed to unmarshal message")

			// Compare the key and value directly with the original ProducerMessage

			assert.Equal(t, msgs[messageIndex].Key, msg.Key)
			assert.Equal(t, msgs[messageIndex].Value, msg.Value)
			messageIndex++
		}
	}

	// Ensure all messages were read
	assert.Equal(t, len(msgs), messageIndex, "Not all messages were read")

	// Cleanup
	err = os.RemoveAll(b.BrokerInfo.BrokerName)
	assert.NoError(t, err)
}

func TestBrokerConcurrentProduce(t *testing.T) {
	persistBatch := 2
	brokerName := "BK1"
	b := NewMockBroker(*logrus.WithField("test", "broker"), brokerName, persistBatch)

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}
	b.AppointAsLeader(context.Background(), &apptRequest)

	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
	}

	numProducers := 2
	iterations := 5
	expectedMessages := numProducers * iterations * len(msgs)
	defer func() {
		_ = os.RemoveAll(b.BrokerInfo.BrokerName)
	}()

	var wg sync.WaitGroup
	for i := 0; i < numProducers; i++ {
		wg.Add(1)
		go func(producerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				_, err := b.ProduceMessage(context.Background(), &proto.ProduceRequest{
					TopicName:   "topic1",
					ProducerId:  fmt.Sprintf("producer%d", producerID),
					PartitionId: 0,
					Messages:    msgs,
				})
				if err != nil {
					t.Errorf("producer %d failed: %v", producerID, err)
				}
			}
		}(i)
	}
	wg.Wait()

	dirPath := filepath.Join(b.BrokerInfo.BrokerName, "topic1-0")
	files, err := os.ReadDir(dirPath)
	assert.NoError(t, err, "Failed to read directory after concurrency test")
	assert.NotEmpty(t, files, "Expected some files to be created")

	// This map will track how many messages each file contains
	fileMessageCount := make(map[string]int)
	var allMessages []*proto.ConsumerMessage

	for _, fileEntry := range files {
		filePath := filepath.Join(dirPath, fileEntry.Name())
		f, err := os.Open(filePath)
		assert.NoError(t, err, "Failed to open file")
		defer f.Close()

		count := 0
		for {
			var length uint32
			err := binary.Read(f, binary.LittleEndian, &length)
			if err == io.EOF {
				break
			}
			assert.NoError(t, err, "Failed to read message length")

			data := make([]byte, length)
			_, err = io.ReadFull(f, data)
			assert.NoError(t, err, "Failed to read entire message data")

			msg := &proto.ConsumerMessage{}
			err = proto1.Unmarshal(data, msg)
			assert.NoError(t, err, "Failed to unmarshal message")

			allMessages = append(allMessages, msg)
			count++
		}
		fileMessageCount[fileEntry.Name()] = count
	}

	// Verify total messages
	assert.Equal(t, expectedMessages, len(allMessages), "Number of read messages does not match expected")

	// Verify each file has the expected number of messages
	// Since PersistBatch=2, each file (except possibly the last) should have 2 messages.
	// We can determine the expected count per file by examining the file name.
	// fileName is like "0000000000.bin", parse the integer and confirm the range.
	for fileName, count := range fileMessageCount {
		baseIndexStr := strings.TrimSuffix(fileName, filepath.Ext(fileName)) // remove .bin
		baseIndex, err := strconv.Atoi(baseIndexStr)
		assert.NoError(t, err, "Failed to parse file index")

		// The offsets for this file range from baseIndex to baseIndex+(PersistBatch-1).
		startOffset := baseIndex
		endOffset := baseIndex + b.persister.NumMessagePerBatch - 1

		// The maximum offset we have is expectedMessages-1 (since offsets start at 0).
		// If the last file might contain fewer messages, handle that:
		maxOffset := expectedMessages - 1
		expectedCount := b.persister.NumMessagePerBatch
		if endOffset > maxOffset {
			// This means it's the last file and may have fewer messages.
			expectedCount = (maxOffset - startOffset) + 1
		}

		assert.Equal(t, expectedCount, count,
			"File %s expected %d messages but got %d", fileName, expectedCount, count)
	}

}

func TestBrokerConsumePersistOffset(t *testing.T) {
	persistBatch := 2
	brokerName := "BK1"
	b := NewMockBroker(*logrus.WithField("test", "broker"), brokerName, persistBatch)
	defer func() {
		_ = os.RemoveAll(b.BrokerInfo.BrokerName)
	}()

	// Produce a few messages
	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
	}

	topicName := "topic1"
	partitionId := int32(0)
	consumerId := "consumer1"
	produceRequest := proto.ProduceRequest{
		TopicName:   topicName,
		ProducerId:  "producer1",
		PartitionId: partitionId,
		Messages:    msgs,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	// Appoint broker as leader
	resp2, err := b.AppointAsLeader(context.Background(), &apptRequest)
	assert.NoError(t, err)
	assert.True(t, resp2.LeaderAppointed)

	// Now produce should succeed
	resp1, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	// Consume all messages in one go
	consumeRequest := proto.ConsumeRequest{
		TopicName:   topicName,
		PartitionId: partitionId,
		ConsumerId:  consumerId,
	}

	resp, err := b.ConsumeMessage(context.Background(), &consumeRequest)
	assert.NoError(t, err, "Consume should succeed")
	assert.Len(t, resp.Records, len(msgs), "Should return all produced messages")

	// Check if offset is persisted
	// After consuming all messages, the offset should be equal to len(msgs).
	expectedOffset := int32(len(msgs))

	// The offset file should be in: brokerName/topicPartitionId-offset/consumerId.bin
	dirPath := filepath.Join(b.BrokerInfo.BrokerName, fmt.Sprintf("%s-%d-offset", topicName, partitionId))
	fileName := fmt.Sprintf("%s.bin", consumerId)
	filePath := filepath.Join(dirPath, fileName)

	// Ensure the file exists
	_, err = os.Stat(filePath)
	assert.NoError(t, err, "Offset file should exist after consumption")

	// Read the offset from the file and verify its value
	f, err := os.Open(filePath)
	assert.NoError(t, err, "Should be able to open offset file")
	defer f.Close()

	var storedOffset int32
	err = binary.Read(f, binary.LittleEndian, &storedOffset)
	assert.NoError(t, err, "Should be able to read offset from file")

	assert.Equal(t, expectedOffset, storedOffset, "Stored offset should match expected offset")

}

func TestOffsetPersist_ConsumeAllAtOnce(t *testing.T) {
	persistBatch := 2
	brokerName := "BK1"
	b := NewMockBroker(*logrus.WithField("test", "broker"), brokerName, persistBatch)
	defer func() {
		_ = os.RemoveAll(b.BrokerInfo.BrokerName)
	}()

	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
	}

	topicName := "topic1"
	partitionId := int32(0)
	consumerId := "consumer_all_once"

	produceRequest := proto.ProduceRequest{
		TopicName:   topicName,
		ProducerId:  "producer1",
		PartitionId: partitionId,
		Messages:    msgs,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	// Appoint broker as leader
	resp2, err := b.AppointAsLeader(context.Background(), &apptRequest)
	assert.NoError(t, err)
	assert.True(t, resp2.LeaderAppointed)

	// Now produce should succeed
	resp1, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	consumeRequest := proto.ConsumeRequest{
		TopicName:   topicName,
		PartitionId: partitionId,
		ConsumerId:  consumerId,
	}

	resp, err := b.ConsumeMessage(context.Background(), &consumeRequest)
	assert.NoError(t, err)
	assert.Len(t, resp.Records, len(msgs))

	// Expect offset file to reflect total messages consumed
	offset := readOffsetFile(t, b.BrokerInfo.BrokerName, topicName, partitionId, consumerId)
	assert.EqualValues(t, len(msgs), offset, "Offset should match number of consumed messages")
}

func TestOffsetPersist_MultipleConsumes(t *testing.T) {
	persistBatch := 2
	brokerName := "BK1"
	b := NewMockBroker(*logrus.WithField("test", "broker"), brokerName, persistBatch)
	defer func() {
		_ = os.RemoveAll(b.BrokerInfo.BrokerName)
	}()

	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
		{Key: "key4", Value: "value4"},
		{Key: "key5", Value: "value5"},
	}

	topicName := "topic1"
	partitionId := int32(0)
	consumerId := "consumer_multi"

	produceRequest := proto.ProduceRequest{
		TopicName:   topicName,
		ProducerId:  "producer1",
		PartitionId: partitionId,
		Messages:    msgs,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	// Producing to a broker that is not leader should fail
	_, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.Error(t, err)

	// Appoint broker as leader
	resp2, err := b.AppointAsLeader(context.Background(), &apptRequest)
	assert.NoError(t, err)
	assert.True(t, resp2.LeaderAppointed)

	// Now produce should succeed
	resp1, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	consumeRequest := proto.ConsumeRequest{
		TopicName:   topicName,
		PartitionId: partitionId,
		ConsumerId:  consumerId,
	}

	// First consume: should return all messages
	resp, err := b.ConsumeMessage(context.Background(), &consumeRequest)
	assert.NoError(t, err)
	assert.Len(t, resp.Records, len(msgs))

	offset := readOffsetFile(t, b.BrokerInfo.BrokerName, topicName, partitionId, consumerId)
	assert.EqualValues(t, len(msgs), offset, "Offset should match total messages consumed")

	// Second consume: no new messages, offset stays the same
	resp, err = b.ConsumeMessage(context.Background(), &consumeRequest)
	assert.NoError(t, err)
	assert.Len(t, resp.Records, 0)

	offset = readOffsetFile(t, b.BrokerInfo.BrokerName, topicName, partitionId, consumerId)
	assert.EqualValues(t, len(msgs), offset, "Offset should remain the same after no new messages")
}

func TestOffsetPersist_MultipleConsumers(t *testing.T) {
	persistBatch := 2
	brokerName := "BK1"
	b := NewMockBroker(*logrus.WithField("test", "broker"), brokerName, persistBatch)
	defer func() {
		_ = os.RemoveAll(b.BrokerInfo.BrokerName)
	}()

	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
	}

	topicName := "topic1"
	partitionId := int32(0)
	consumerIdA := "consumerA"
	consumerIdB := "consumerB"

	produceRequest := proto.ProduceRequest{
		TopicName:   topicName,
		ProducerId:  "producer1",
		PartitionId: partitionId,
		Messages:    msgs,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	// Appoint broker as leader
	resp2, err := b.AppointAsLeader(context.Background(), &apptRequest)
	assert.NoError(t, err)
	assert.True(t, resp2.LeaderAppointed)

	// Now produce should succeed
	resp1, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	// consumerA consumes all messages
	resp, err := b.ConsumeMessage(context.Background(), &proto.ConsumeRequest{
		TopicName:   topicName,
		PartitionId: partitionId,
		ConsumerId:  consumerIdA,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Records, len(msgs))
	offsetA := readOffsetFile(t, b.BrokerInfo.BrokerName, topicName, partitionId, consumerIdA)
	assert.EqualValues(t, len(msgs), offsetA)

	// consumerB is a new consumer, also gets all messages from start
	resp, err = b.ConsumeMessage(context.Background(), &proto.ConsumeRequest{
		TopicName:   topicName,
		PartitionId: partitionId,
		ConsumerId:  consumerIdB,
	})
	assert.NoError(t, err)
	assert.Len(t, resp.Records, len(msgs))
	offsetB := readOffsetFile(t, b.BrokerInfo.BrokerName, topicName, partitionId, consumerIdB)
	assert.EqualValues(t, len(msgs), offsetB)

	// Check that each consumer's offset file is independent
}

func TestOffsetPersist_ConcurrentConsume(t *testing.T) {
	persistBatch := 2
	brokerName := "BK1"
	b := NewMockBroker(*logrus.WithField("test", "broker"), brokerName, persistBatch)
	defer func() {
		_ = os.RemoveAll(b.BrokerInfo.BrokerName)
	}()

	// Produce messages
	totalMessages := 20
	msgs := make([]*proto.ProducerMessage, totalMessages)
	for i := 0; i < totalMessages; i++ {
		msgs[i] = &proto.ProducerMessage{Key: fmt.Sprintf("key%d", i), Value: fmt.Sprintf("value%d", i)}
	}

	topicName := "topic1"
	partitionId := int32(0)

	produceRequest := proto.ProduceRequest{
		TopicName:   topicName,
		ProducerId:  "producer1",
		PartitionId: partitionId,
		Messages:    msgs,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	// Appoint broker as leader
	resp2, err := b.AppointAsLeader(context.Background(), &apptRequest)
	assert.NoError(t, err)
	assert.True(t, resp2.LeaderAppointed)

	// Now produce should succeed
	resp1, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	// Number of concurrent consumers
	numConsumers := 5

	// Each consumer will read until no more messages are available
	consumeUntilDone := func(consumerId string) {
		consumeReq := proto.ConsumeRequest{
			TopicName:   topicName,
			PartitionId: partitionId,
			ConsumerId:  consumerId,
		}

		for {
			resp, err := b.ConsumeMessage(context.Background(), &consumeReq)
			assert.NoError(t, err, "Consume should not fail for consumer %s", consumerId)

			// If no records returned, we've reached the end for this consumer
			if len(resp.Records) == 0 {
				break
			}

			// Just keep consuming until empty
		}

		// At the end, the offset file should be created and have offset == totalMessages
		offset := readOffsetFile(t, b.BrokerInfo.BrokerName, topicName, partitionId, consumerId)
		assert.EqualValues(t, totalMessages, offset, "Consumer %s offset should match total messages", consumerId)
	}

	var wg sync.WaitGroup
	wg.Add(numConsumers)

	// Start multiple consumers concurrently
	for i := 0; i < numConsumers; i++ {
		consumerId := fmt.Sprintf("consumer%d", i)
		go func(cid string) {
			defer wg.Done()
			consumeUntilDone(cid)
		}(consumerId)
	}

	wg.Wait()

}

func TestBrokerRecovery(t *testing.T) {
	persistBatch := 2
	brokerName := "broker_recovery_test"

	// Create a new broker instance with the specified brokerName
	b := NewMockBroker(*logrus.WithField("test", brokerName), brokerName, persistBatch)

	defer func() {
		_ = os.RemoveAll(brokerName)
	}()

	// Produce messages to the broker
	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
	}

	topicName := "topic1"
	partitionId := int32(0)
	consumerId := "consumer1"

	produceRequest := proto.ProduceRequest{
		TopicName:   topicName,
		ProducerId:  "producer1",
		PartitionId: partitionId,
		Messages:    msgs,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	// Appoint broker as leader
	resp2, err := b.AppointAsLeader(context.Background(), &apptRequest)
	assert.NoError(t, err)
	assert.True(t, resp2.LeaderAppointed)

	// Now produce should succeed
	resp1, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	// Simulate broker shutdown by setting b to nil
	b = nil

	// Re-instantiate the broker to simulate a restart
	b2 := NewMockBroker(*logrus.WithField("test", brokerName), brokerName, persistBatch)

	// Consume messages from the restarted broker
	consumeRequest := proto.ConsumeRequest{
		TopicName:   topicName,
		PartitionId: partitionId,
		ConsumerId:  consumerId,
	}

	resp, err := b2.ConsumeMessage(context.Background(), &consumeRequest)
	assert.NoError(t, err)
	assert.Len(t, resp.Records, len(msgs))

	// Verify that the consumed messages match the produced messages
	for i, msg := range resp.Records {
		assert.Equal(t, msgs[i].Key, msg.GetKey())
		assert.Equal(t, msgs[i].Value, msg.GetValue())
	}
}

func TestBrokerInitialization_LoadPersistedMessages(t *testing.T) {
	persistBatch := 2
	brokerName := "broker_initialization_test"

	// Step 1: Create a new broker instance and produce messages
	b := NewMockBroker(*logrus.WithField("test", brokerName), brokerName, persistBatch)
	defer func() {
		_ = os.RemoveAll(brokerName)
	}()

	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
		{Key: "key4", Value: "value4"},
	}

	topicName := "topic1"
	partitionId := int32(0)
	produceRequest := proto.ProduceRequest{
		TopicName:   topicName,
		ProducerId:  "producer1",
		PartitionId: partitionId,
		Messages:    msgs,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	// Appoint broker as leader
	resp2, err := b.AppointAsLeader(context.Background(), &apptRequest)
	assert.NoError(t, err)
	assert.True(t, resp2.LeaderAppointed)

	// Now produce should succeed
	resp1, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	// Step 2: Verify messages are persisted correctly
	// (Optional) You can check the files on disk if needed
	// For this test, we'll proceed to simulate a broker restart

	// Step 3: Simulate broker shutdown
	b = nil

	// Step 4: Re-instantiate the broker to simulate a restart
	b2 := NewMockBroker(*logrus.WithField("test", brokerName), brokerName, persistBatch)

	// Step 5: Verify that the broker loaded the correct number of messages
	topicPartitionID := fmt.Sprintf("%s-%d", topicName, partitionId)
	mapShard := b2.data.ShardForKey(topicPartitionID)
	mapShard.RLock()
	loadedMessages, ok := mapShard.Items[topicPartitionID]
	mapShard.RUnlock()

	assert.True(t, ok, "Partition data should be loaded")
	assert.Equal(t, len(msgs), len(loadedMessages), "Loaded message count should match produced message count")

	// Step 6: Optionally, log the loaded messages for verification
	for i, msg := range loadedMessages {
		b2.logger.Printf("Loaded message %d: Offset=%d, Key=%s, Value=%s", i, msg.GetOffset(), msg.GetKey(), msg.GetValue())
	}
}

func TestBrokerInitialization_NoMessagesConsumed(t *testing.T) {
	persistBatch := 2
	brokerName := "broker_no_messages_consumed_test"

	// Step 1: Create a new broker instance and produce messages
	b := NewMockBroker(*logrus.WithField("test", brokerName), brokerName, persistBatch)
	defer func() {
		_ = os.RemoveAll(brokerName)
	}()

	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
		{Key: "key4", Value: "value4"},
	}

	topicName := "topic1"
	partitionId := int32(0)
	consumerId := "consumer1"

	produceRequest := proto.ProduceRequest{
		TopicName:   topicName,
		ProducerId:  "producer1",
		PartitionId: partitionId,
		Messages:    msgs,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	// Appoint broker as leader
	resp2, err := b.AppointAsLeader(context.Background(), &apptRequest)
	assert.NoError(t, err)
	assert.True(t, resp2.LeaderAppointed)

	// Now produce should succeed
	resp1, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	// Step 2: Simulate broker shutdown
	b = nil

	// Step 3: Re-instantiate the broker to simulate a restart
	b2 := NewMockBroker(*logrus.WithField("test", brokerName), brokerName, persistBatch)
	b2.logger.Printf("Broker %s initialized", brokerName)

	// Step 4: Verify that the consumer offset is 0
	offset := readOffsetFile(t, brokerName, topicName, partitionId, consumerId)
	assert.EqualValues(t, 0, offset, "Offset should be 0 when no messages are consumed")
}

func TestBrokerInitialization_AllMessagesConsumed(t *testing.T) {
	persistBatch := 2
	brokerName := "broker_all_messages_consumed_test"

	// Step 1: Create a new broker instance and produce messages
	b := NewMockBroker(*logrus.WithField("test", brokerName), brokerName, persistBatch)
	defer func() {
		_ = os.RemoveAll(brokerName)
	}()

	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
		{Key: "key4", Value: "value4"},
	}

	topicName := "topic1"
	partitionId := int32(0)
	consumerId := "consumer1"

	produceRequest := proto.ProduceRequest{
		TopicName:   topicName,
		ProducerId:  "producer1",
		PartitionId: partitionId,
		Messages:    msgs,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:   "topic1",
		PartitionId: 0,
	}

	// Appoint broker as leader
	resp2, err := b.AppointAsLeader(context.Background(), &apptRequest)
	assert.NoError(t, err)
	assert.True(t, resp2.LeaderAppointed)

	// Now produce should succeed
	resp1, err := b.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	// Step 2: Consume all messages
	consumeRequest := proto.ConsumeRequest{
		TopicName:   topicName,
		PartitionId: partitionId,
		ConsumerId:  consumerId,
	}

	resp, err := b.ConsumeMessage(context.Background(), &consumeRequest)
	assert.NoError(t, err)
	assert.Len(t, resp.Records, len(msgs), "Should consume all messages")

	// Verify that the consumed messages match the produced messages
	for i, msg := range resp.Records {
		assert.Equal(t, msgs[i].Key, msg.GetKey(), "Message key should match")
		assert.Equal(t, msgs[i].Value, msg.GetValue(), "Message value should match")
	}

	// Verify that the consumer offset is updated to the number of messages
	offsetBeforeShutdown := readOffsetFile(t, brokerName, topicName, partitionId, consumerId)
	assert.EqualValues(t, len(msgs), offsetBeforeShutdown, "Offset should equal total number of messages after consuming all")

	// Step 3: Simulate broker shutdown
	b = nil

	// Step 4: Re-instantiate the broker to simulate a restart
	b2 := NewMockBroker(*logrus.WithField("test", brokerName), brokerName, persistBatch)

	// Step 5: Verify that the consumer offset is loaded correctly
	offsetAfterRestart := readOffsetFile(t, brokerName, topicName, partitionId, consumerId)
	assert.EqualValues(t, len(msgs), offsetAfterRestart, "Offset should be the same after restart")
	println("Offset after restart:", offsetAfterRestart)
	println("Offset returned from broker:", b2.GetConsumerOffset(consumerId, topicName, partitionId))

	// Step 6: Attempt to consume again; should get no new messages
	resp, err = b2.ConsumeMessage(context.Background(), &consumeRequest)
	assert.NoError(t, err)
	assert.Len(t, resp.Records, 0, "No new messages should be available")

	// Offset should remain the same
	finalOffset := readOffsetFile(t, brokerName, topicName, partitionId, consumerId)
	assert.EqualValues(t, len(msgs), finalOffset, "Final offset should still be the same after consuming all messages")
}

func TestReplication(t *testing.T) {
	persistBatch := 2
	logger := logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	logger.SetOutput(os.Stdout)
	entry := logrus.NewEntry(logger)

	// Create a leader broker and a replica broker in-memory
	leader := NewMockBroker(*entry, "leader", persistBatch)
	replica := NewMockBroker(*entry, "replica", persistBatch)

	defer func() {
		_ = os.RemoveAll("leader")
		_ = os.RemoveAll("replica")
	}()

	// Configure the leader to replicate to the replica for a given topic-partition
	topicName := "topic1"
	partitionId := int32(0)
	topicPartitionID := fmt.Sprintf("%s-%d", topicName, partitionId)

	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
		{Key: "key4", Value: "value4"},
	}

	BrokerInfoProtoReplica := &proto.BrokerInfoProto{
		BrokerId: replica.BrokerInfo.BrokerName,
		Addr:     replica.BrokerInfo.NodeAddr,
	}

	apptRequest := proto.AppointmentRequest{
		TopicName:      topicName,
		PartitionId:    partitionId,
		ReplicaBrokers: []*proto.BrokerInfoProto{BrokerInfoProtoReplica},
	}

	produceRequest := proto.ProduceRequest{
		TopicName:   topicName,
		ProducerId:  "producer1",
		PartitionId: partitionId,
		Messages:    msgs,
	}

	// Appoint broker as leader
	resp2, err := leader.AppointAsLeader(context.Background(), &apptRequest)
	assert.NoError(t, err)
	assert.True(t, resp2.LeaderAppointed)

	// Now produce should succeed
	resp1, err := leader.ProduceMessage(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	// Manually replicate messages to the replica
	consumerMsgs := convertToConsumerMessages(msgs)
	replicateRequest := &proto.ReplicateRequest{
		TopicName:   topicName,
		PartitionId: partitionId,
		Messages:    consumerMsgs,
	}

	_, err = replica.ReplicateMessage(context.Background(), replicateRequest)
	assert.NoError(t, err, "Failed to replicate messages to the replica")

	// Retrieve the messages from the replica's data store
	mapShard := replica.GetData().ShardForKey(topicPartitionID)
	mapShard.RLock()
	defer mapShard.RUnlock()
	topicPartition := mapShard.Items[topicPartitionID]

	// Assert that the replica has received all messages
	assert.EqualValues(t, 4, len(topicPartition), "Replica should have received 4 messages")

	// verify that the messages are correct
	for i, msg := range msgs {
		replicatedMsg := topicPartition[i]
		assert.EqualValues(t, msg.Key, string(replicatedMsg.Key), "Message keys should match")
		assert.EqualValues(t, msg.Value, string(replicatedMsg.Value), "Message values should match")
	}

	// Verify that the messages are persisted to disk
	verifyMessagesPersistedToDisk(t, replica.BrokerInfo.BrokerName, topicName, partitionId, consumerMsgs)

}
