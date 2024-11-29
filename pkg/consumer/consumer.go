package kueue

import (
	"context"
	"fmt"
	"sync"

	"kueue/kueue/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Consumer represents a message consumer in the message queue.
type Consumer struct {
	mu sync.RWMutex

	consumerID string

	controllerAddr   string
	controllerConn   *grpc.ClientConn
	controllerClient proto.ControllerServiceClient

	brokerConns   map[string]*grpc.ClientConn
	brokerClients map[string]proto.BrokerServiceClient

	offsets map[string]int64 // partition_id -> offset
}

// NewConsumer creates a new Consumer connected to the specified controller.
func NewConsumer(controllerAddr, consumerID string) (*Consumer, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(controllerAddr, opts...)
	if err != nil {
		return nil, err
	}

	controllerClient := proto.NewControllerServiceClient(conn)

	return &Consumer{
		consumerID:       consumerID,
		controllerAddr:   controllerAddr,
		controllerConn:   conn,
		controllerClient: controllerClient,
		brokerConns:      make(map[string]*grpc.ClientConn),
		brokerClients:    make(map[string]proto.BrokerServiceClient),
		offsets:          make(map[string]int64),
	}, nil
}

// Subscribe subscribes the consumer to a topic and fetches partition assignments.
func (c *Consumer) Subscribe(ctx context.Context, topic string) error {
	// Get partition metadata from controller
	req := &proto.ConsumerTopicInfoRequest{
		TopicName:  topic,
		ConsumerId: c.consumerID,
	}

	resp, err := c.controllerClient.GetTopicConsumer(ctx, req)
	if err != nil {
		return err
	}

	if len(resp.Partitions) == 0 {
		return fmt.Errorf("no partitions available for topic %s", topic)
	}

	// Initialize offsets for each partition
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, partition := range resp.Partitions {
		if _, exists := c.offsets[partition.PartitionId]; !exists {
			c.offsets[partition.PartitionId] = 0 // Start from offset 0 or load from storage
		}
	}

	return nil
}

// Consume starts consuming messages from the subscribed partitions.
func (c *Consumer) Consume(ctx context.Context, topic string, handler func(key, value string) error) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var wg sync.WaitGroup
	errCh := make(chan error, len(c.offsets))

	for partitionID := range c.offsets {
		wg.Add(1)
		go func(partitionID string) {
			defer wg.Done()
			if err := c.consumePartition(ctx, topic, partitionID, handler); err != nil {
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

// consumePartition consumes messages from a single partition.
func (c *Consumer) consumePartition(ctx context.Context, topic, partitionID string, handler func(key, value string) error) error {
	offset := c.getOffset(partitionID)

	// Get partition metadata from controller
	req := &proto.ConsumerTopicInfoRequest{
		TopicName:  topic,
		ConsumerId: c.consumerID,
	}

	resp, err := c.controllerClient.GetTopicConsumer(ctx, req)
	if err != nil {
		return err
	}

	// Find the partition metadata
	var partitionMeta *proto.PartitionMetadata
	for _, partition := range resp.Partitions {
		if partition.PartitionId == partitionID {
			partitionMeta = partition
			break
		}
	}

	if partitionMeta == nil {
		return fmt.Errorf("partition %s not found", partitionID)
	}

	brokerClient, err := c.getBrokerClient(partitionMeta.Leader)
	if err != nil {
		return err
	}

	consumeReq := &proto.ConsumeRequest{
		Topic:       topic,
		PartitionId: partitionID,
		Offset:      offset,
		MaxMessages: 10, // Adjust as needed
	}

	stream, err := brokerClient.ConsumeMessages(ctx, consumeReq)
	if err != nil {
		return err
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			break
		}

		if err := handler(msg.Key, msg.Value); err != nil {
			return err
		}

		c.updateOffset(partitionID, msg.Offset+1)

		if err := c.commitOffset(ctx, topic, partitionID, msg.Offset+1); err != nil {
			return err
		}
	}

	return nil
}

// getBrokerClient retrieves or creates a gRPC client for the specified broker.
func (c *Consumer) getBrokerClient(nodeInfo *proto.NodeInfo) (proto.BrokerServiceClient, error) {
	address := fmt.Sprintf("%s:%d", nodeInfo.Address, nodeInfo.Port)

	c.mu.RLock()
	client, exists := c.brokerClients[address]
	c.mu.RUnlock()
	if exists {
		return client, nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	client, exists = c.brokerClients[address]
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

	c.brokerConns[address] = conn
	c.brokerClients[address] = brokerClient

	return brokerClient, nil
}

// getOffset retrieves the current offset for a partition.
func (c *Consumer) getOffset(partitionID string) int64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.offsets[partitionID]
}

// updateOffset updates the offset for a partition.
func (c *Consumer) updateOffset(partitionID string, offset int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.offsets[partitionID] = offset
}

// commitOffset commits the offset to the broker.
func (c *Consumer) commitOffset(ctx context.Context, topic, partitionID string, offset int64) error {
	// Get partition metadata from controller
	req := &proto.ConsumerTopicInfoRequest{
		TopicName:  topic,
		ConsumerId: c.consumerID,
	}

	resp, err := c.controllerClient.GetTopicConsumer(ctx, req)
	if err != nil {
		return err
	}

	// Find the partition metadata
	var partitionMeta *proto.PartitionMetadata
	for _, partition := range resp.Partitions {
		if partition.PartitionId == partitionID {
			partitionMeta = partition
			break
		}
	}

	if partitionMeta == nil {
		return fmt.Errorf("partition %s not found", partitionID)
	}

	brokerClient, err := c.getBrokerClient(partitionMeta.Leader)
	if err != nil {
		return err
	}

	commitReq := &proto.CommitOffsetRequest{
		ConsumerId:  c.consumerID,
		Topic:       topic,
		PartitionId: partitionID,
		Offset:      offset,
	}

	commitResp, err := brokerClient.CommitOffset(ctx, commitReq)
	if err != nil {
		return err
	}

	if !commitResp.Success {
		return fmt.Errorf("failed to commit offset: %s", commitResp.ErrorMessage)
	}

	return nil
}
