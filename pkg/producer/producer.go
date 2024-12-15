package producer

import (
	"context"
	"fmt"
	"sync"

	"kueue/kueue/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Partitioner func(key string, numPartitions int) int

func defaultPartitioner(key string, numPartitions int) int {
	return int(Hash(key)) % numPartitions
}

type roundRobin struct {
	mu      sync.Mutex
	counter int
}

func (rr *roundRobin) partition(_ string, numPartitions int) int {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	part := rr.counter % numPartitions
	rr.counter++
	return part
}

// Simple hash function for partitioning.
func Hash(s string) uint32 {
	// FNV-1a hash
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h = (h ^ uint32(s[i])) * 16777619
	}
	return h
}

type Producer struct {
	mu sync.RWMutex

	controllerAddr   string
	controllerConn   *grpc.ClientConn
	controllerClient proto.ControllerServiceClient

	brokerConns   map[string]*grpc.ClientConn
	brokerClients map[string]proto.BrokerServiceClient

	partitioner Partitioner
	rrPartition *roundRobin
}

// Creates a new Producer connected to the specified controller.
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
		rrPartition:      &roundRobin{},
	}, nil
}

// SetPartitioner switches the partitioning strategy.
// If "roundrobin" is provided, it uses round robin; otherwise it defaults to the hash-based partitioner.
func (p *Producer) SetPartitioner(strategy string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch strategy {
	case "roundrobin":
		p.partitioner = p.rrPartition.partition
	default:
		p.partitioner = defaultPartitioner
	}
}

// CreateTopic attempts to create a topic with the given name, number of partitions, and replication factor.
// If the topic already exists, it returns the existing metadata.
func (p *Producer) CreateTopic(ctx context.Context, topic string, numPartitions, replicationFactor int) (*proto.ProducerTopicResponse, error) {
	numParts := int32(numPartitions)
	repFactor := int32(replicationFactor)

	req := &proto.ProducerTopicRequest{
		TopicName:         topic,
		NumPartitions:     &numParts,  // optional
		ReplicationFactor: &repFactor, // optional
	}

	resp, err := p.controllerClient.GetTopic(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to create or get topic %s: %w", topic, err)
	}

	return resp, nil
}

// produceOnce attempts to produce messages without retries.
func (p *Producer) Produce(ctx context.Context, topic string, producerID string, keys []string, values []string) error {
	if len(keys) != len(values) {
		return fmt.Errorf("mismatched keys and values length")
	}
	if len(keys) == 0 {
		return fmt.Errorf("no messages to produce")
	}

	metadata, err := p.getTopicMetadata(ctx, topic)
	if err != nil {
		return err
	}

	if len(metadata.Partitions) == 0 {
		return fmt.Errorf("no partitions available for topic %s", topic)
	}

	numPartitions := len(metadata.Partitions)

	partitionBatches := make(map[int][]*proto.ProducerMessage)
	for i := 0; i < len(keys); i++ {
		key := keys[i]
		val := values[i]
		partID := p.partitioner(key, numPartitions)
		if partID < 0 || partID >= numPartitions {
			return fmt.Errorf("invalid partition index %d", partID)
		}
		partitionBatches[partID] = append(partitionBatches[partID], &proto.ProducerMessage{
			Key:   key,
			Value: val,
		})
	}

	// Send requests to each partition's leader broker
	for partID, msgs := range partitionBatches {
		partitionMeta := metadata.Partitions[partID]
		leaderAddr := partitionMeta.LeaderAddress
		if leaderAddr == "" {
			return fmt.Errorf("no leader found for partition %d", partitionMeta.PartitionId)
		}

		brokerClient, err := p.getBrokerClient(leaderAddr)
		if err != nil {
			return err
		}

		req := &proto.ProduceRequest{
			TopicName:   topic,
			ProducerId:  producerID,
			PartitionId: partitionMeta.PartitionId,
			Messages:    msgs,
		}

		resp, err := brokerClient.ProduceMessage(ctx, req)
		if err != nil {
			return fmt.Errorf("error producing to broker %s: %v", leaderAddr, err)
		}

		// Check the response for success or other info if needed
		if resp.BaseOffset < 0 {
			return fmt.Errorf("received invalid base_offset %d from broker %s for topic %s partition %d",
				resp.BaseOffset, leaderAddr, topic, partitionMeta.PartitionId)
		}
	}

	return nil
}

// getTopicMetadata fetches partition metadata for the topic from the controller.
func (p *Producer) getTopicMetadata(ctx context.Context, topic string) (*proto.ProducerTopicResponse, error) {
	req := &proto.ProducerTopicRequest{
		TopicName: topic,
	}
	resp, err := p.controllerClient.GetTopic(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// getBrokerClient retrieves or creates a gRPC client for the specified broker address.
func (p *Producer) getBrokerClient(address string) (proto.BrokerServiceClient, error) {
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
