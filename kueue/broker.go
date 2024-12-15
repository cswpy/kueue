package kueue

import (
	"context"
	// "encoding/binary"
	// "io"
	// "io/fs"
	// "sort"
	// "strings"

	// "encoding/gob"
	"fmt"
	"kueue/kueue/proto"
	"log"
	"os"
	// "path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	// "golang.org/x/text/message"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	// proto1 "google.golang.org/protobuf/proto"
)

var (
	MAX_BATCH_SIZE int = 20
	MAP_SHARD_SIZE int = 8
)

type Broker struct {
	proto.UnimplementedBrokerServiceServer

	BrokerInfo     *BrokerInfo
	ControllerAddr string
	client         proto.ControllerServiceClient
	logger         logrus.Entry
	data           *ConcurrentMap[string, []*proto.ConsumerMessage] // topic_partition_id -> list of records, save protobuf messages directly for simplicity
	consumerOffset map[string]int32                                 // consumer_id_topic_partition_id -> offset
	offsetLock     sync.RWMutex
	messageCountMu sync.Mutex
}

func (b *Broker) GetConsumerOffset(consumerID string, topicName string, partitionID int32) int32 {
    topicPartitionID := fmt.Sprintf("%s-%d", topicName, partitionID)
    offsetLookupKey := fmt.Sprintf("%s-%s", consumerID, topicPartitionID)
    b.offsetLock.RLock()
    defer b.offsetLock.RUnlock()
    return b.consumerOffset[offsetLookupKey]
}

// NewMockBroker creates a new broker with an in-memory data store, incapable of communicating with gRPC services
func NewMockBroker(logger logrus.Entry, brokerName string, persistBatch int) *Broker {
    brokerInfo := &BrokerInfo{
        BrokerName:   brokerName, // Use the provided brokerName
        NodeAddr:     "localhost:50051",
        PersistBatch: persistBatch,
    }

    broker := &Broker{
        BrokerInfo:     brokerInfo,
        logger:         logger,
        data:           NewConcurrentMap[string, []*proto.ConsumerMessage](MAP_SHARD_SIZE),
        consumerOffset: make(map[string]int32),
    }

    // Load persisted messages and offsets
    if err := broker.loadPersistedData(); err != nil {
        broker.logger.Printf("Failed to load persisted data: %v", err)
    }

    if err := broker.loadConsumerOffsets(); err != nil {
        broker.logger.Printf("Failed to load consumer offsets: %v", err)
    }

    return broker
}

func NewBroker(info *BrokerInfo, controllerAddr string, logger logrus.Entry) (*Broker, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(controllerAddr, opts...)
	if err != nil {
		return nil, err
	}
	client := proto.NewControllerServiceClient(conn)

	// Register with the controller
	_, err = client.RegisterBroker(context.Background(), &proto.RegisterBrokerRequest{
		BrokerId:      info.BrokerName,
		BrokerAddress: info.NodeAddr,
	})

	if err != nil {
		return nil, err
	}

	// Create directory for broker folder
	dirPath := info.BrokerName
    err = os.MkdirAll(dirPath, 0755)
    if err != nil {
        log.Fatalf("Failed to create directory: %v", err)
        return nil, err
    }

	broker := &Broker{
		BrokerInfo:     info,
		ControllerAddr: controllerAddr,
		client:         client,
		logger:         logger,
		data:           NewConcurrentMap[string, []*proto.ConsumerMessage](MAP_SHARD_SIZE),
		consumerOffset: make(map[string]int32),
	}

	// Load persisted messages and offsets
    if err := broker.loadPersistedData(); err != nil {
        broker.logger.Printf("Failed to load persisted data: %v", err)
        return nil, err
    }

    if err := broker.loadConsumerOffsets(); err != nil {
        broker.logger.Printf("Failed to load consumer offsets: %v", err)
        return nil, err
    }

	return broker,nil
}

func (b *Broker) Produce(ctx context.Context, req *proto.ProduceRequest) (*proto.ProduceResponse, error) {
	b.logger.WithField("Topic", DBroker).Debugf("Received Produce request: %v", req)

	partitionID := int(req.PartitionId)
	topicPartitionID := fmt.Sprintf("%s-%d", req.TopicName, partitionID)

	// Wait for lock before loading data
	mapShard := b.data.ShardForKey(topicPartitionID)
	mapShard.Lock()
	defer mapShard.Unlock()

	var topicPartition []*proto.ConsumerMessage
	var ok bool
	if topicPartition, ok = mapShard.Items[topicPartitionID]; !ok {
		mapShard.Items[topicPartitionID] = make([]*proto.ConsumerMessage, 0)
	}

	// Getting Offset from the last record in the partition
	baseOffset := int32(0)
	topicPartitionLength := len(topicPartition)
	if topicPartitionLength > 0 {
		baseOffset = topicPartition[topicPartitionLength-1].Offset + 1
	}
	nextOffset := baseOffset

	// Append records to partition, since topicPartition may be reallocated in append, we need to update the map
	for _, msg := range req.Messages {
		consumerMsg := &proto.ConsumerMessage{
			Offset:    nextOffset,
			Timestamp: time.Now().Unix(),
			Key:       msg.Key,
			Value:     msg.Value,
		}
		topicPartition = append(topicPartition, consumerMsg)
		nextOffset++
		b.persistData(topicPartitionID, consumerMsg)
		
	}

	mapShard.Items[topicPartitionID] = topicPartition

	return &proto.ProduceResponse{
		TopicName:  req.TopicName,
		BaseOffset: baseOffset,
	}, nil
}



// TODO: handle the case where new consumers start consuming from the beginning of the topic-partition, which is not in main memory but on disk
// Consume returns a batch of messages from a topic-partition, it checks whether the topic-partition exists in the broker's data
// if not, it means the request is probably unauthorized/meant for another broker; then checks if there is any message to consume;
// it creates an offset for new consumers
func (b *Broker) Consume(ctx context.Context, req *proto.ConsumeRequest) (*proto.ConsumeResponse, error) {
	b.logger.WithField("Topic", DBroker).Debugf("Received Consume request: %v", req)

	topicPartitionID := fmt.Sprintf("%s-%d", req.TopicName, req.PartitionId)

	// Wait for read lock
	mapShard := b.data.ShardForKey(topicPartitionID)
	mapShard.RLock()
	defer mapShard.RUnlock()

	var topicPartition []*proto.ConsumerMessage
	var ok bool

	// Check if the topic-partition exists in the broker's data
	if topicPartition, ok = mapShard.Items[topicPartitionID]; !ok {
		err := fmt.Errorf("topic-partition %s not found in broker %v data", topicPartitionID, b.BrokerInfo.BrokerName)
		b.logger.Fatal(err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	// Check topic-partition data size

	if len(topicPartition) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "topic-partition is empty")
	}

	b.offsetLock.Lock()
	defer b.offsetLock.Unlock()

	// Create offset for new consumers
	offsetLookupKey := fmt.Sprintf("%s-%s", req.ConsumerId, topicPartitionID)
	if _, ok := b.consumerOffset[offsetLookupKey]; !ok {
		b.consumerOffset[offsetLookupKey] = 0
	}

	currMsgOffset := b.consumerOffset[offsetLookupKey]

	baseMsgOffset := topicPartition[0].Offset
	beginIndex := int(currMsgOffset - baseMsgOffset)
	endIndex := min(beginIndex+MAX_BATCH_SIZE, len(topicPartition))

	numMsgs := endIndex - beginIndex
	batchMsgs := make([]*proto.ConsumerMessage, numMsgs)

	copy(batchMsgs, topicPartition[beginIndex:endIndex])
	resp := &proto.ConsumeResponse{
		TopicName: req.TopicName,
		Records:   batchMsgs,
	}

	newOffset := int32(endIndex)
	b.consumerOffset[offsetLookupKey] = newOffset
	// Persist the updated offset
    b.persistConsumerOffset(topicPartitionID, req.ConsumerId, newOffset)

	return resp, nil
}


func (b *Broker) SendHeartbeat() {

	for {
		b.logger.WithField("Topic", DBroker).Infof("Sending heartbeat to controller.")
		_, err := b.client.Heartbeat(context.Background(), &proto.HeartbeatRequest{
			BrokerId: b.BrokerInfo.BrokerName,
		})
		if err != nil {
			b.logger.WithField("Topic", DBroker).Errorf("Error sending heartbeat: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
}