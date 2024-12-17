package consumer

import (
	"context"
	"fmt"
	"sync"

	"kueue/kueue/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Consumer struct {
	mu sync.RWMutex

	consumerID     string
	controllerAddr string

	controllerConn   *grpc.ClientConn
	controllerClient proto.ControllerServiceClient

	brokerConns   map[string]*grpc.ClientConn
	brokerClients map[string]proto.BrokerServiceClient

	// Map of topic to a map of partitionID->offset
	offsets map[string]map[int32]int64

	// Map of topic to assigned partitions
	assignedPartitions map[string][]int32

	// Map of (topic, partition) -> leader address (cached)
	partitionLeaders map[string]map[int32]string
}

// NewConsumer creates a new Consumer connected to the specified controller.
func NewConsumer(controllerAddr, consumerID string) (*Consumer, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(controllerAddr, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to controller: %w", err)
	}

	controllerClient := proto.NewControllerServiceClient(conn)

	return &Consumer{
		consumerID:         consumerID,
		controllerAddr:     controllerAddr,
		controllerConn:     conn,
		controllerClient:   controllerClient,
		brokerConns:        make(map[string]*grpc.ClientConn),
		brokerClients:      make(map[string]proto.BrokerServiceClient),
		offsets:            make(map[string]map[int32]int64),
		assignedPartitions: make(map[string][]int32),
		partitionLeaders:   make(map[string]map[int32]string),
	}, nil
}

// Subscribe subscribes the consumer to a topic, receiving a partition assignment from the controller.
func (c *Consumer) Subscribe(ctx context.Context, topic string) error {
	req := &proto.SubscribeRequest{
		TopicName:  topic,
		ConsumerId: c.consumerID,
	}

	resp, err := c.controllerClient.Subscribe(ctx, req)
	if err != nil {
		return fmt.Errorf("subscribe failed: %w", err)
	}

	partition := resp.Partition
	if partition == nil {
		return fmt.Errorf("no partition assigned for topic %s", topic)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.offsets[topic]; !exists {
		c.offsets[topic] = make(map[int32]int64)
	}

	// Assign the partition if not already assigned
	if !containsInt32(c.assignedPartitions[topic], partition.PartitionId) {
		c.assignedPartitions[topic] = append(c.assignedPartitions[topic], partition.PartitionId)
		// Start from offset 0, or in a real scenario, load from persisted storage.
		if _, exists := c.offsets[topic][partition.PartitionId]; !exists {
			c.offsets[topic][partition.PartitionId] = 0
		}
	}

	// Cache the leader address for this partition
	if _, exists := c.partitionLeaders[topic]; !exists {
		c.partitionLeaders[topic] = make(map[int32]string)
	}

	if partition.Leader == nil {
		return fmt.Errorf("partition %d for topic %s has nil leader info", partition.PartitionId, topic)
	}
	if partition.Leader.Addr == "" {
		return fmt.Errorf("partition %d for topic %s has empty leader address", partition.PartitionId, topic)
	}
	c.partitionLeaders[topic][partition.PartitionId] = partition.Leader.Addr

	return nil
}

// Consume consumes messages from all assigned partitions of a given topic.
// The handler is called for each message (key-value pair).
func (c *Consumer) Consume(ctx context.Context, topic string, handler func(key, value string) error) error {
	c.mu.RLock()
	partitions := append([]int32{}, c.assignedPartitions[topic]...)
	c.mu.RUnlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(partitions))

	for _, partitionID := range partitions {
		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()
			if err := c.consumePartition(ctx, topic, pid, handler); err != nil {
				errCh <- err
			}
		}(partitionID)
	}

	wg.Wait()
	close(errCh)

	if len(errCh) > 0 {
		return <-errCh
	}
	return nil
}

// consumePartition consumes messages from a single partition by sending a ConsumeRequest to the broker.
// It will fetch messages from the current offset onwards.
func (c *Consumer) consumePartition(ctx context.Context, topic string, partitionID int32, handler func(key, value string) error) error {
	leaderAddr := c.getPartitionLeader(topic, partitionID)
	if leaderAddr == "" {
		// If we have no leader cached, try subscribing again (or handle error)
		if err := c.Subscribe(ctx, topic); err != nil {
			return fmt.Errorf("failed to re-subscribe to get leader info: %w", err)
		}
		leaderAddr = c.getPartitionLeader(topic, partitionID)
		if leaderAddr == "" {
			return fmt.Errorf("no leader found for topic %s partition %d", topic, partitionID)
		}
	}

	brokerClient, err := c.getBrokerClient(leaderAddr)
	if err != nil {
		return fmt.Errorf("failed to get broker client for leader %s: %w", leaderAddr, err)
	}

	consumeReq := &proto.ConsumeRequest{
		TopicName:   topic,
		PartitionId: partitionID,
		ConsumerId:  c.consumerID,
	}

	consumeResp, err := brokerClient.ConsumeMessage(ctx, consumeReq)
	if err != nil {
		return fmt.Errorf("consume request failed: %w", err)
	}

	for _, msg := range consumeResp.Records {
		if err := handler(msg.Key, msg.Value); err != nil {
			return fmt.Errorf("handler error at offset %d for topic %s partition %d: %w", msg.Offset, topic, partitionID, err)
		}

		c.updateOffset(topic, partitionID, int64(msg.Offset)+1)
	}

	return nil
}

// getBrokerClient returns a BrokerServiceClient for the given address, caching connections.
func (c *Consumer) getBrokerClient(address string) (proto.BrokerServiceClient, error) {
	c.mu.RLock()
	client, exists := c.brokerClients[address]
	c.mu.RUnlock()

	if exists {
		return client, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if client, exists := c.brokerClients[address]; exists {
		return client, nil
	}

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to broker at %s: %w", address, err)
	}

	brokerClient := proto.NewBrokerServiceClient(conn)
	c.brokerConns[address] = conn
	c.brokerClients[address] = brokerClient
	return brokerClient, nil
}

func (c *Consumer) getPartitionLeader(topic string, partitionID int32) string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if leaders, ok := c.partitionLeaders[topic]; ok {
		return leaders[partitionID]
	}
	return ""
}

func (c *Consumer) getOffset(topic string, partitionID int32) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if topicOffsets, exists := c.offsets[topic]; exists {
		return topicOffsets[partitionID]
	}
	return 0
}

func (c *Consumer) updateOffset(topic string, partitionID int32, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.offsets[topic]; !exists {
		c.offsets[topic] = make(map[int32]int64)
	}
	c.offsets[topic][partitionID] = offset
}

func containsInt32(arr []int32, val int32) bool {
	for _, x := range arr {
		if x == val {
			return true
		}
	}
	return false
}
