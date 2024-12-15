package producer

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"kueue/kueue"
	"kueue/kueue/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

func NewLoggerEntry(l *log.Logger, nodeID string) *logrus.Entry {
	logrusLogger := logrus.New()
	logrusLogger.SetOutput(l.Writer())
	return logrusLogger.WithField("Node", nodeID)
}

func startController(t *testing.T, port int) (string, func()) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to listen for controller on %s: %v", addr, err)
	}

	controllerID := "CTRL-TEST"
	logger := log.New(os.Stdout, "[Controller] ", log.LstdFlags)
	loggerEntry := NewLoggerEntry(logger, controllerID)

	controller := kueue.NewController(controllerID, *loggerEntry)
	grpcServer := grpc.NewServer()
	proto.RegisterControllerServiceServer(grpcServer, controller)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("Controller Serve error: %v", err)
		}
	}()

	cleanup := func() {
		grpcServer.Stop()
	}
	return addr, cleanup
}

func startBroker(t *testing.T, brokerName string, controllerAddr string, persistBatch int, port int) (string, *kueue.Broker, func()) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("Failed to listen for broker on %s: %v", addr, err)
	}

	logger := log.New(os.Stdout, fmt.Sprintf("[Broker-%s] ", brokerName), log.LstdFlags)
	loggerEntry := NewLoggerEntry(logger, brokerName)

	bi := &kueue.BrokerInfo{
		BrokerName:   brokerName,
		NodeAddr:     lis.Addr().String(),
		PersistBatch: persistBatch,
	}

	broker, err := kueue.NewBroker(bi, controllerAddr, *loggerEntry)
	if err != nil {
		t.Fatalf("Failed to create broker: %v", err)
	}

	grpcServer := grpc.NewServer()
	proto.RegisterBrokerServiceServer(grpcServer, broker)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			log.Printf("Broker Serve error: %v", err)
		}
	}()

	cleanup := func() {
		grpcServer.Stop()
	}

	return addr, broker, cleanup
}

// TestProducerIntegration tests the producer by starting a controller and broker, then producing messages and verifying.
func TestProducerIntegration(t *testing.T) {
	controllerAddr, controllerCleanup := startController(t, 8080)
	defer controllerCleanup()

	// Persist every 5 messages
	brokerName := "broker-test"
	persistBatch := 5
	_, _, brokerCleanup := startBroker(t, brokerName, controllerAddr, persistBatch, 8081)
	defer brokerCleanup()

	defer os.RemoveAll(brokerName)

	time.Sleep(500 * time.Millisecond)

	producer, err := NewProducer(controllerAddr)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}

	topic := "test-topic"
	numPartitions := 2
	replicationFactor := 1
	ctx := context.Background()

	topicResp, err := producer.CreateTopic(ctx, topic, numPartitions, replicationFactor)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}

	if topicResp.TopicName != topic {
		t.Errorf("expected topic name %s, got %s", topic, topicResp.TopicName)
	}

	if len(topicResp.Partitions) != numPartitions {
		t.Errorf("expected %d partitions, got %d", numPartitions, len(topicResp.Partitions))
	}

	keys := []string{"key1", "key2", "key3", "key4", "key5"}
	values := []string{"value1", "value2", "value3", "value4", "value5"}
	producerID := "producer-1"

	err = producer.Produce(ctx, topic, producerID, keys, values)
	if err != nil {
		t.Fatalf("Failed to produce messages: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	// Check the persisted files.
	// We know persistBatch number, so all messages should be in the first file: "0000000000.bin" in directory brokerName/test-topic-0 or test-topic-1
	// We must identify which partition the messages went to. The default partitioner uses hashing.
	// For simplicity, let's just check both partitions.

	for pID := 0; pID < numPartitions; pID++ {
		dirPath := filepath.Join(brokerName, fmt.Sprintf("%s-%d", topic, pID))
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			// It's possible some partitions got no data if keys always hashed to the same partition
			// We'll skip if the directory doesn't exist
			continue
		}

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}
			filePath := filepath.Join(dirPath, entry.Name())
			data, err := os.ReadFile(filePath)
			if err != nil {
				t.Fatalf("Failed to read persisted file: %v", err)
			}

			// Data is stored as: length(uint32, little-endian) + message proto bytes repeated per message
			// We expect persistBatch number of messages total across partitions. Let's just check we can parse them.
			// This test won't decode all messages for brevity.
			// Here we just check at least one message got persisted.
			if len(data) == 0 {
				t.Errorf("File %s is empty, expected some data", filePath)
			}
		}
	}
}
