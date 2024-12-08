package kueue

import (
	"context"
	"fmt"
	"kueue/kueue/proto"
	"time"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Broker struct {
	proto.UnimplementedBrokerServiceServer
	BrokerInfo     *BrokerInfo
	ControllerAddr string
	client         proto.ControllerServiceClient
	logger         logrus.Entry
	data           map[string][]*proto.ConsumerMessage // topic_partition_id -> list of records, save protobuf messages directly for simplicity
	consumerOffset map[string]map[string]int           // consumer_id -> topic_partition_id -> offset
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

	// Create topic if it doesn't exist
	if _, ok := b.data[req.TopicName]; !ok {
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
	partitionID := int(req.PartitionId)
	topicPartitionID := fmt.Sprintf("%s-%d", req.TopicName, partitionID)
	topicPartition := b.data[topicPartitionID]
	// Getting Offset from the last record in the partition
	baseOffset := int32(0)
	topicPartitionLength := len(topicPartition)
	if topicPartitionLength > 0 {
		baseOffset = topicPartition[topicPartitionLength-1].Offset + 1
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
		topicPartition = append(topicPartition, consumerMsg)
		nextOffset++
	}

	return &proto.ProduceResponse{
		TopicName:  req.TopicName,
		BaseOffset: baseOffset,
	}, nil
}

func (b *Broker) Consume(ctx context.Context, req *proto.ConsumeRequest) (*proto.ConsumeResponse, error) {
	b.logger.WithField("Topic", DBroker).Debugf("Received Consume request: %v", req)
	return &proto.ConsumeResponse{}, nil
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
