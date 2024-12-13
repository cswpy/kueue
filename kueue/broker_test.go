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

	"github.com/puzpuzpuz/xsync"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	proto1 "google.golang.org/protobuf/proto"
)

// func TestBrokerProduceConsume(t *testing.T) {

// 	b := Broker{
// 		logger: *logrus.WithField("test", "broker"),
// 		Data:   xsync.NewMap(),
// 		BrokerInfo: &BrokerInfo{
// 			BrokerName: "BK1",
// PersistBatch: 100,
// 		},
// 	}

// 	msgs := []*proto.ProducerMessage{
// 		{Key: "key1", Value: "value1"},
// 		{Key: "key2", Value: "value2"},
// 		{Key: "key3", Value: "value3"},
// 		{Key: "key4", Value: "value4"},
// 		{Key: "key5", Value: "value5"},
// 	}

// 	produceRequest := proto.ProduceRequest{
// 		TopicName:   "topic1",
// 		ProducerId:  "producer1",
// 		PartitionId: 0,
// 		Messages:    msgs,
// 	}

// 	resp1, err := b.Produce(context.Background(), &produceRequest)
// 	assert.NoError(t, err)
// 	assert.EqualValues(t, resp1.BaseOffset, 0)

// 	consumeRequest := proto.ConsumeRequest{
// 		TopicName:   "topic1",
// 		PartitionId: 0,
// 		ConsumerId:  "consumer1",
// 	}

// 	resp2, err := b.Consume(context.Background(), &consumeRequest)
// 	assert.NoError(t, err)
// 	assert.Equal(t, resp2.TopicName, "topic1")
// 	assert.Len(t, resp2.Records, len(msgs))
// 	for i := 0; i < len(msgs); i++ {
// 		assert.Equal(t, resp2.Records[i].Key, msgs[i].Key)
// 		assert.Equal(t, resp2.Records[i].Value, msgs[i].Value)
// 	}
// }



func TestBrokerProducePersist(t *testing.T) {
    b := Broker{
        logger: *logrus.WithField("test", "broker"),
        Data:   xsync.NewMap(),
        BrokerInfo: &BrokerInfo{
            BrokerName:   "BK1",
            PersistBatch: 2,
        },
    }

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
    b := Broker{
        logger: *logrus.WithField("test", "broker-concurrency"),
        Data:   xsync.NewMap(),
        BrokerInfo: &BrokerInfo{
            BrokerName:   "BK1",
            PersistBatch: 2,
        },
    }

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










// func TestBroker(t *testing.T) {
// 	lis := bufconn.Listen(1024 * 1024)

// 	s := grpc.NewServer()
// 	defer s.Stop()
// 	b := makeBroker(t)

// 	proto.RegisterBrokerServiceServer(s, b)

// 	go func() {
// 		if err := s.Serve(lis); err != nil {
// 			panic(err)
// 		}
// 	}()

// 	// See https://stackoverflow.com/questions/78485578/how-to-use-the-bufconn-package-with-grpc-newclient
// 	conn, err := grpc.NewClient("passthrough://buffnet", grpc.WithDefaultCallOptions())

// 	assert.NoError(t, err)

// 	client := proto.NewBrokerServiceClient(conn)

// 	// client.ProduceMessage(context.Background(), &proto.ProduceRequest{
// 	// 	TopicName: "topic1",
// 	// 	ProducerId: "producer1",
// 	// 	Message: &proto.Message{
// 	// })

// }
