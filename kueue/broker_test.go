package kueue

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"kueue/kueue/proto"
	"os"
	"path/filepath"
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

    // Prepare a set of messages for multiple producers
    msgs := []*proto.ProducerMessage{
        {Key: "key1", Value: "value1"},
        {Key: "key2", Value: "value2"},
        {Key: "key3", Value: "value3"},
        {Key: "key4", Value: "value4"},
        {Key: "key5", Value: "value5"},
    }

    // Number of concurrent producers
    numProducers := 10
    var wg sync.WaitGroup

    // Start concurrent producers
    for i := 0; i < numProducers; i++ {
        wg.Add(1)
        go func(producerID int) {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                // Each producer sends a batch of messages
                // You can vary the topic, partition, or just use the same for contention
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

    // Wait for all producers to finish
    wg.Wait()

    // If no panic occurred and no data races are reported when using `-race`,
    // it suggests that the code is thread-safe.
    // However, you can also add assertions to check final data states if needed.

    // Verify some data after concurrency:
    dirPath := filepath.Join(b.BrokerInfo.BrokerName, "topic1-0")
    _, err := os.ReadDir(dirPath)
    assert.NoError(t, err, "Failed to read directory after concurrency test")

    // Optionally, clean up
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
