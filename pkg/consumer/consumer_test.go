package consumer

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"kueue/kueue"
	"kueue/kueue/proto"
	"kueue/pkg/producer"

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
		BrokerName: brokerName,
		NodeAddr:   lis.Addr().String(),
	}

	broker, err := kueue.NewBroker(bi, controllerAddr, persistBatch, *loggerEntry)
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

func TestConsumerIntegration(t *testing.T) {
	controllerAddr, controllerCleanup := startController(t, 8080)
	defer controllerCleanup()

	brokerName := "broker-test-consumer"
	persistBatch := 5
	_, _, brokerCleanup := startBroker(t, brokerName, controllerAddr, persistBatch, 8081)
	defer brokerCleanup()
	defer os.RemoveAll(brokerName)

	time.Sleep(500 * time.Millisecond)

	producerClient, err := producer.NewProducer(controllerAddr)
	if err != nil {
		t.Fatalf("Failed to create producer: %v", err)
	}

	topic := "consumer-test-topic"
	numPartitions := 2
	replicationFactor := 1
	ctx := context.Background()

	topicResp, err := producerClient.CreateTopic(ctx, topic, numPartitions, replicationFactor)
	if err != nil {
		t.Fatalf("Failed to create topic: %v", err)
	}
	if topicResp.TopicName != topic {
		t.Errorf("expected topic name %s, got %s", topic, topicResp.TopicName)
	}

	keys := []string{"ckey1", "ckey2", "ckey3", "ckey4", "ckey5"}
	values := []string{"cvalue1", "cvalue2", "cvalue3", "cvalue4", "cvalue5"}
	producerID := "producer-consumer-test"

	err = producerClient.Produce(ctx, topic, producerID, keys, values)
	if err != nil {
		t.Fatalf("Failed to produce messages: %v", err)
	}

	time.Sleep(300 * time.Millisecond)

	// Create a consumer and subscribe to all partitions of the topic
	consumerID := "consumer-1"
	consumerClient, err := NewConsumer(controllerAddr, consumerID)
	if err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}

	// Since the controller assigns one partition per Subscribe call,
	// call Subscribe numPartitions times to get all partitions.
	for i := 0; i < numPartitions; i++ {
		err = consumerClient.Subscribe(ctx, topic)
		if err != nil {
			t.Fatalf("Failed to subscribe consumer to topic %s: %v", topic, err)
		}
	}

	// Now the consumer has all partitions assigned. Let's consume.
	var consumedMessages []string
	mu := &sync.Mutex{}

	handler := func(key, value string) error {
		mu.Lock()
		defer mu.Unlock()
		consumedMessages = append(consumedMessages, fmt.Sprintf("%s:%s", key, value))
		return nil
	}

	err = consumerClient.Consume(ctx, topic, handler)
	if err != nil {
		t.Fatalf("Failed to consume messages: %v", err)
	}

	// Verify that all produced messages were consumed
	expectedSet := make(map[string]struct{})
	for i, k := range keys {
		msgStr := fmt.Sprintf("%s:%s", k, values[i])
		expectedSet[msgStr] = struct{}{}
	}

	receivedSet := make(map[string]struct{})
	for _, msg := range consumedMessages {
		receivedSet[msg] = struct{}{}
	}

	// Check if all produced messages are present in consumed messages
	for msg := range expectedSet {
		if _, found := receivedSet[msg]; !found {
			t.Errorf("expected message %s not found in consumed messages", msg)
		}
	}

	// Check that the data was persisted as expected
	for pID := 0; pID < numPartitions; pID++ {
		dirPath := filepath.Join(brokerName, fmt.Sprintf("%s-%d", topic, pID))
		entries, err := os.ReadDir(dirPath)
		if err != nil {
			// It's possible no data for a particular partition if all keys hashed to another partition.
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
			if len(data) == 0 {
				t.Errorf("File %s is empty, expected some data", filePath)
			}
		}
	}

	if t.Failed() {
		t.Logf("Consumed messages: %#v", consumedMessages)
	}
}
