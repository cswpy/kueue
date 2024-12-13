package kueue

import (
	"context"
	// "encoding/gob"
	"fmt"
	"kueue/kueue/proto"
	"os"
	"path/filepath"
	"sync"
	"time"
	"log"

	"github.com/puzpuzpuz/xsync"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	proto1 "google.golang.org/protobuf/proto"
)

var (
	MAX_BATCH_SIZE int32 = 20
)

type Broker struct {
	proto.UnimplementedBrokerServiceServer

	BrokerInfo     *BrokerInfo
	infoLock       sync.Mutex
	ControllerAddr string
	client         proto.ControllerServiceClient
	logger         logrus.Entry
	Data           *xsync.Map                 // topic_partition_id -> list of records, save protobuf messages directly for simplicity; uses xsync.Map for concurrent access
	ConsumerOffset map[string]map[string]int // consumer_id -> topic_partition_id -> offset
	offsetLock     sync.RWMutex
	MessageCount   map[string]int    
	messageCountMu sync.Mutex 
}

func NewBroker(info *BrokerInfo, controllerAddr string, logger logrus.Entry,) (*Broker, error) {
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

	return &Broker{
		BrokerInfo:     info,
		ControllerAddr: controllerAddr,
		client:         client,
		Data: 		 	xsync.NewMap(), // Initialize the xsync.Map
		ConsumerOffset: make(map[string]map[string]int),
		logger:         logger,
		MessageCount:   make(map[string]int),
	}, nil
}

func (b *Broker) Produce(ctx context.Context, req *proto.ProduceRequest) (*proto.ProduceResponse, error) {
	b.logger.WithField("Topic", DBroker).Debugf("Received Produce request: %v", req)

	partitionID := int(req.PartitionId)
	topicPartitionID := fmt.Sprintf("%s-%d", req.TopicName, partitionID)

	b.messageCountMu.Lock()
    defer b.messageCountMu.Unlock()
	
	topicPartition, _ := b.Data.LoadOrStore(topicPartitionID, make([]*proto.ConsumerMessage, 0))
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
		go b.persistData(topicPartitionID, consumerMsg)
		
	}

	b.Data.Store(topicPartitionID, topicPartitionData)



	return &proto.ProduceResponse{
		TopicName:  req.TopicName,
		BaseOffset: baseOffset,
	}, nil
}


func (b *Broker) persistData(topicPartitionId string, msg *proto.ConsumerMessage) {
	b.messageCountMu.Lock()
    defer b.messageCountMu.Unlock()

    dirPath := filepath.Join(b.BrokerInfo.BrokerName, topicPartitionId)
    err := os.MkdirAll(dirPath, 0755)
    if err != nil {
        b.logger.Fatalf("Failed to create directory: %v", err)
        return
    }

	
    // Serialize the message
    dataBytes, err := proto1.Marshal(msg)
    if err != nil {
        b.logger.Printf("Failed to marshal message: %v", err)
        return
    }


    messageCount := b.MessageCount[topicPartitionId]
    fileIndex := (messageCount / b.BrokerInfo.PersistBatch) * b.BrokerInfo.PersistBatch
    fileName := fmt.Sprintf("%010d.bin", fileIndex)
	filePath := filepath.Join(dirPath, fileName)



	// Open the file in append mode
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		b.logger.Printf("Failed to open file %s: %v", filePath, err)
		return
	}
	defer file.Close()

	_, err = file.Write(dataBytes)
	if err != nil {
		b.logger.Printf("Failed to write to file %s: %v", filePath, err)
		return
	}

	b.MessageCount[topicPartitionId]++


    
}


// func (b *Broker) persistConsumerOffset(topicPartitionId string) {
//     dirPath := b.BrokerInfo.BrokerName+"/"+topicPartitionId
//     err := os.MkdirAll(dirPath, 0755)
//     if err != nil {
//         b.logger.Fatalf("Failed to create directory: %v", err)
//         return
//     }

// 	fileName := fmt.Sprintf("%s-offset.bin", topicPartitionId)
//     filePath := filepath.Join(dirPath, fileName)

//     file, err := os.Create(filePath)
//     if err != nil {
//         b.logger.Fatalf("Failed to create file: %v", err)
//         return
//     }
//     defer file.Close()

//     encoder := gob.NewEncoder(file)
//     err = encoder.Encode(b.ConsumerOffset)
//     if err != nil {
//         b.logger.Printf("Failed to encode consumer offsets: %v", err)
//     }
// }


func (b *Broker) Consume(ctx context.Context, req *proto.ConsumeRequest) (*proto.ConsumeResponse, error) {
	b.logger.WithField("Topic", DBroker).Debugf("Received Consume request: %v", req)

	b.offsetLock.Lock()
	defer b.offsetLock.Unlock()

	if _, ok := b.ConsumerOffset[req.ConsumerId]; !ok {
		return nil, status.Error(codes.NotFound, "Consumer not found")
	}

	currConsumerOffset := b.ConsumerOffset[req.ConsumerId]
	topicPartitionId := fmt.Sprintf("%s-%d", req.TopicName, req.PartitionId)
	if _, ok := currConsumerOffset[topicPartitionId]; !ok {
		return nil, status.Error(codes.NotFound, "Topic-partition not found in broker")
	}

	currMsgOffset := int32(currConsumerOffset[topicPartitionId])

	topicPartition, ok := b.Data.Load(topicPartitionId)

	if !ok {
		err := fmt.Errorf("topic-partition %s not found in broker %v data", topicPartitionId, b.BrokerInfo.BrokerName)
		b.logger.Fatal(err)
		return nil, status.Error(codes.Internal, err.Error())
	}

	topicPartitionData := topicPartition.([]*proto.ConsumerMessage)

	if len(topicPartitionData) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "topic-partition is empty")
	}

	baseMsgOffset := topicPartitionData[0].Offset
	beginIndex := currMsgOffset - baseMsgOffset

	batchMsgs := make([]*proto.ConsumerMessage, MAX_BATCH_SIZE)

	endIndex := min(int(beginIndex+MAX_BATCH_SIZE), len(topicPartitionData))

	copy(batchMsgs, topicPartitionData[beginIndex:endIndex])
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