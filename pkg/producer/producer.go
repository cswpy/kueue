package kueue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"kueue/kueue/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Producer represents a message producer in the message queue.
type Producer struct {
	mu sync.RWMutex

	controllerAddr   string
	controllerConn   *grpc.ClientConn
	controllerClient proto.ControllerServiceClient

	brokerConns   map[string]*grpc.ClientConn
	brokerClients map[string]proto.BrokerServiceClient

	partitioner func(key string, numPartitions int) int
}

// NewProducer creates a new Producer connected to the specified controller.
func NewProducer(controllerAddr string) (*Producer, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(controllerAddr, opts...)
	if err != nil {
		return nil, err
	}

	controllerClient := proto.NewControllerServiceClient(conn)

	return &Producer{
		controllerAddr:   controllerAddr,
		controllerConn:   conn,
		controllerClient: controllerClient,
		brokerConns:      make(map[string]*grpc.ClientConn),
		brokerClients:    make(map[string]proto.BrokerServiceClient),
		partitioner:      defaultPartitioner,
	}, nil
}

// defaultPartitioner assigns a partition based on the message key.
func defaultPartitioner(key string, numPartitions int) int {
	return int(Hash(key)) % numPartitions
}

// Hash is a simple hash function for partitioning.
func Hash(s string) uint32 {
	// FNV-1a hash
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h = (h ^ uint32(s[i])) * 16777619
	}
	return h
}

// CreateTopic requests the controller to create a new topic.
func (p *Producer) CreateTopic(ctx context.Context, topicName string, partitionCount int, replicationFactor int) error {
	req := &proto.CreateTopicRequest{
		TopicName:         topicName,
		PartitionCount:    int32(partitionCount),
		ReplicationFactor: int32(replicationFactor),
	}

	resp, err := p.controllerClient.CreateTopic(ctx, req)
	if err != nil {
		return err
	}

	if !resp.Success {
		return fmt.Errorf("failed to create topic: %s", resp.ErrorMessage)
	}

	return nil
}

// Produce sends a message to the appropriate broker partition.
func (p *Producer) Produce(ctx context.Context, topic string, key string, value string) error {
	const maxRetries = 3
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		err := p.produceOnce(ctx, topic, key, value)
		if err == nil {
			return nil
		}
		lastErr = err

		// Handle specific errors for retries
		if isRetriableError(err) {
			time.Sleep(time.Duration(i) * time.Second)
			continue
		} else {
			return err
		}
	}

	return fmt.Errorf("failed to produce message after %d retries: %v", maxRetries, lastErr)
}

// produceOnce attempts to produce a message without retries.
func (p *Producer) produceOnce(ctx context.Context, topic string, key string, value string) error {
	metadata, err := p.getTopicMetadata(ctx, topic)
	if err != nil {
		return err
	}

	if len(metadata.Partitions) == 0 {
		return fmt.Errorf("no partitions available for topic %s", topic)
	}

	// Decide partition
	numPartitions := len(metadata.Partitions)
	partitionIndex := p.partitioner(key, numPartitions)
	if partitionIndex < 0 || partitionIndex >= numPartitions {
		return fmt.Errorf("invalid partition index %d", partitionIndex)
	}

	partition := metadata.Partitions[partitionIndex]

	// Get leader broker info
	leaderBroker := partition.Leader
	if leaderBroker == nil {
		return fmt.Errorf("no leader found for partition %s", partition.PartitionId)
	}

	// Get or create broker client
	brokerClient, err := p.getBrokerClient(leaderBroker)
	if err != nil {
		return err
	}

	req := &proto.ProduceRequest{
		Topic:       topic,
		PartitionId: partition.PartitionId,
		Key:         key,
		Value:       value,
	}

	resp, err := brokerClient.ProduceMessage(ctx, req)
	if err != nil {
		return err
	}

	if resp.Success {
		return nil
	}
	return fmt.Errorf("failed to produce message: %s", resp.ErrorMessage)
}

// getTopicMetadata fetches partition metadata for the topic from the controller.
func (p *Producer) getTopicMetadata(ctx context.Context, topic string) (*proto.ProducerTopicInfoResponse, error) {
	req := &proto.ProducerTopicInfoRequest{
		TopicName: topic,
	}

	resp, err := p.controllerClient.GetTopicProducer(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

// getBrokerClient retrieves or creates a gRPC client for the specified broker.
func (p *Producer) getBrokerClient(nodeInfo *proto.NodeInfo) (proto.BrokerServiceClient, error) {
	address := fmt.Sprintf("%s:%d", nodeInfo.Address, nodeInfo.Port)

	p.mu.RLock()
	client, exists := p.brokerClients[address]
	p.mu.RUnlock()
	if exists {
		return client, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	client, exists = p.brokerClients[address]
	if exists {
		return client, nil
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, err
	}

	brokerClient := proto.NewBrokerServiceClient(conn)

	p.brokerConns[address] = conn
	p.brokerClients[address] = brokerClient

	return brokerClient, nil
}

// isRetriableError determines if an error is retriable.
func isRetriableError(err error) bool {
	// Implement logic to check for retriable errors
	// For example, network errors, leader not available, etc.
	// For simplicity, we'll retry on any error here
	return true
}
