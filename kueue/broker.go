package kueue

import (
	"context"
	"fmt"
	"kueue/kueue/proto"
	"sync"
	"time"

	"github.com/puzpuzpuz/xsync"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var (
	MAX_BATCH_SIZE = 20
)

type Broker struct {
	proto.UnimplementedBrokerServiceServer

	BrokerInfo     *BrokerInfo
	infoLock       sync.Mutex
	ControllerAddr string
	client         proto.ControllerServiceClient
	logger         logrus.Entry
	data           xsync.Map                 // topic_partition_id -> list of records, save protobuf messages directly for simplicity; uses xsync.Map for concurrent access
	consumerOffset map[string]map[string]int // consumer_id -> topic_partition_id -> offset
	offsetLock     sync.RWMutex
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

	return &Broker{
		BrokerInfo:     info,
		ControllerAddr: controllerAddr,
		client:         client,
		logger:         logger,
	}, nil
}

func (b *Broker) Produce(ctx context.Context, req *proto.ProduceRequest) (*proto.ProduceResponse, error) {
	b.logger.WithField("Topic", DBroker).Debugf("Received Produce request: %v", req)

	b.infoLock.Lock()

	// Create topic if it doesn't exist
	if _, ok := b.BrokerInfo.HostedTopics[req.TopicName]; !ok {
		b.BrokerInfo.HostedTopics[req.TopicName] = &TopicInfo{
			TopicName:         req.TopicName,
			TopicPartitions:   make(map[int]*PartitionInfo),
			ReplicationFactor: 1,
		}
	}
	topic := b.BrokerInfo.HostedTopics[req.TopicName]

	// Create partition if it doesn't exist
	if _, ok := topic.TopicPartitions[int(req.PartitionId)]; !ok {
		topic.TopicPartitions[int(req.PartitionId)] = &PartitionInfo{
			PartitionID: int(req.PartitionId),
			IsLeader:    true,
		}
	}

	b.infoLock.Unlock()

	partitionID := int(req.PartitionId)
	topicPartitionID := fmt.Sprintf("%s-%d", req.TopicName, partitionID)
	topicPartition, _ := b.data.LoadOrStore(topicPartitionID, make([]*proto.ConsumerMessage, 0))

	topicPartitionData := topicPartition.([]*proto.ConsumerMessage)

	// Getting Offset from the last record in the partition
	baseOffset := int32(0)
	topicPartitionLength := len(topicPartitionData)
	if topicPartitionLength > 0 {
		baseOffset = topicPartitionData[topicPartitionLength-1].Offset + 1
	}
	nextOffset := baseOffset

	// Append records to partition
	for _, msg := range req.Messages {
		consumerMsg := &proto.ConsumerMessage{
			Offset:    nextOffset,
			Timestamp: time.Now().Unix(),
			Key:       msg.Key,
			Value:     msg.Value,
		}
		topicPartitionData = append(topicPartitionData, consumerMsg)
		nextOffset++
	}

	b.data.Store(topicPartitionID, topicPartitionData)

	return &proto.ProduceResponse{
		TopicName:  req.TopicName,
		BaseOffset: baseOffset,
	}, nil
}

func (b *Broker) Consume(ctx context.Context, req *proto.ConsumeRequest) (*proto.ConsumeResponse, error) {
	b.logger.WithField("Topic", DBroker).Debugf("Received Consume request: %v", req)

	b.offsetLock.Lock()
	defer b.offsetLock.Unlock()

	if _, ok := b.consumerOffset[req.ConsumerId]; !ok {
		return nil, status.Error(codes.NotFound, "Consumer not found in broker")
	}

	currConsumerOffset := b.consumerOffset[req.ConsumerId]
	topicPartitionId := fmt.Sprintf("%s-%d", req.TopicName, req.PartitionId)
	if _, ok := currConsumerOffset[topicPartitionId]; !ok {
		return nil, status.Error(codes.NotFound, "Topic-partition not found in broker")
	}

	baseOffset := currConsumerOffset[topicPartitionId]

	topicPartition, ok := b.data.Load(topicPartitionId)

	if !ok {
		err := fmt.Errorf("topic-partition %s not found in broker %v data", topicPartitionId, b.BrokerInfo.BrokerName)
		b.logger.Fatal(err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	topicPartitionData := topicPartition.([]*proto.ConsumerMessage)

	batchMsgs := make([]*proto.ConsumerMessage, MAX_BATCH_SIZE)

	endOffset := min(baseOffset+MAX_BATCH_SIZE, len(topicPartitionData))

	copy(batchMsgs, topicPartitionData[baseOffset:endOffset])
	resp := &proto.ConsumeResponse{
		TopicName: req.TopicName,
		Records:   batchMsgs,
	}
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
