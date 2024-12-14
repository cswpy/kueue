package kueue

import (
	"context"
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
	"time"

	// "github.com/puzpuzpuz/xsync"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	proto1 "google.golang.org/protobuf/proto"
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
	persistBatch := 2
	b := NewMockBroker(*logrus.WithField("test", "broker"), persistBatch)

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

	// Cleanup
    err = os.RemoveAll(b.BrokerInfo.BrokerName)
    assert.NoError(t, err)
}

func TestBrokerConsumeEmptyPartition(t *testing.T) {
	persistBatch := 2
	b := NewMockBroker(*logrus.WithField("test", "broker"), persistBatch)

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

	// Cleanup
    err = os.RemoveAll(b.BrokerInfo.BrokerName)
    assert.NoError(t, err)

}

func TestBrokerMPSC(t *testing.T) {
	var (
		numProducer           = 2
		numMessagePerProducer = 10
		numMessagePerRequest  = 5
		persistBatch		  = 2
	)
	numBatch := numMessagePerProducer / numMessagePerRequest
	b := NewMockBroker(*logrus.WithField("test", "broker"), persistBatch)
	msgs := generateMessages(numProducer * numMessagePerProducer)

	var wg sync.WaitGroup

	for i := 0; i < numProducer; i++ {
		go func(i int, msgs []*proto.ProducerMessage) {
			wg.Add(1)
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
		// Cleanup
		err = os.RemoveAll(b.BrokerInfo.BrokerName)
		assert.NoError(t, err)
		return
	default:
		resp1, err = b.Consume(context.Background(), &consumeRequest)
		assert.NoError(t, err)
		arr = append(arr, resp1.Records...)
		// Cleanup
		err = os.RemoveAll(b.BrokerInfo.BrokerName)
		assert.NoError(t, err)
	}

	checkMessageContent(t, msgs, arr)
	//fmt.Println(arr)
	checkMessageMetadata(t, arr, 0)

	
}

func TestBrokerProducePersist(t *testing.T) {
    // b := Broker{
    //     logger: *logrus.WithField("test", "broker"),
    //     Data:   xsync.NewMap(),
    //     BrokerInfo: &BrokerInfo{
    //         BrokerName:   "BK1",
    //         PersistBatch: 2,
    //     },
    // }
	persistBatch := 2
	b := NewMockBroker(*logrus.WithField("test", "broker"), persistBatch)

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
    resp1, err := b.Produce(context.Background(), &produceRequest)
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
    // b := Broker{
    //     logger: *logrus.WithField("test", "broker-concurrency"),
    //     Data:   xsync.NewMap(),
    //     BrokerInfo: &BrokerInfo{
    //         BrokerName:   "BK1",
    //         PersistBatch: 2,
    //     },
    // }

	persistBatch := 2
	b := NewMockBroker(*logrus.WithField("test", "broker"), persistBatch)

    msgs := []*proto.ProducerMessage{
        {Key: "key1", Value: "value1"},
        {Key: "key2", Value: "value2"},
        {Key: "key3", Value: "value3"},
    }

    numProducers := 2
    iterations := 5
    expectedMessages := numProducers * iterations * len(msgs)

    var wg sync.WaitGroup
    for i := 0; i < numProducers; i++ {
        wg.Add(1)
        go func(producerID int) {
            defer wg.Done()
            for j := 0; j < iterations; j++ {
                _, err := b.Produce(context.Background(), &proto.ProduceRequest{
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
        endOffset := baseIndex + b.BrokerInfo.PersistBatch - 1

        // The maximum offset we have is expectedMessages-1 (since offsets start at 0).
        // If the last file might contain fewer messages, handle that:
        maxOffset := expectedMessages - 1
        expectedCount := b.BrokerInfo.PersistBatch
        if endOffset > maxOffset {
            // This means it's the last file and may have fewer messages.
            expectedCount = (maxOffset - startOffset) + 1
        }

        assert.Equal(t, expectedCount, count, 
            "File %s expected %d messages but got %d", fileName, expectedCount, count)
    }

    // Optional: Clean up
    err = os.RemoveAll(b.BrokerInfo.BrokerName)
    assert.NoError(t, err)
}
