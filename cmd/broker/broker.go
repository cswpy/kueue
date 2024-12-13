package main

import (
	"context"
	"flag"
	"fmt"
	"sync"

	// "fmt"
	"kueue/kueue"
	"net"

	// "os"
	// "path/filepath"

	proto "kueue/kueue/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	// proto1 "google.golang.org/protobuf/proto"
)

var (
	brokerServiceAddr = flag.String(
		"broker-address",
		"127.0.0.1:8081",
		"host ip address of the broker service in the format of host:port",
	)
	brokerName = flag.String(
		"broker-name",
		"",
		"unique name of the broker",
	)
	controllerAddr = flag.String(
		"controller-address",
		"",
		"host ip address of the controller service in the format of host:port",
	)
	persistBatch = flag.Int(
        "persist-batch",
        0,
        "number of messages to persist for each topicPartitionId",
    )


)

func main() {
	flag.Parse()
	if persistBatch == nil || *persistBatch == 0 {
        logrus.Fatalf("persist batch is required.")
    }
	if brokerName == nil || *brokerName == "" {
		logrus.Fatalf("Broker name is required.")
	}
	if controllerAddr == nil || *controllerAddr == "" {
		logrus.Fatalf("Controller address is required.")
	}
	lis, err := net.Listen("tcp", *brokerServiceAddr)
	if err != nil {
		logrus.Fatalf("Failed to listen: %v", err)
	}
	logger := logrus.WithField("Node", *brokerName)
	logger.WithField("Topic", kueue.DBroker).Infof("Broker service listening on %s", *brokerServiceAddr)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	bi := &kueue.BrokerInfo{
		BrokerName:   *brokerName,
		NodeAddr:     *brokerServiceAddr,
		PersistBatch: *persistBatch,
	}
	logger.Printf("Connecting to controller at %s", *controllerAddr)
	broker, err := kueue.NewBroker(bi, *controllerAddr, *logger)
	

	if err != nil {
		logrus.Fatalf("Failed to create broker: %v", err)
	}

	proto.RegisterBrokerServiceServer(grpcServer, broker)
	// grpcServer.Serve(lis)
	go func() {
        if err := grpcServer.Serve(lis); err != nil {
            logrus.Fatalf("Failed to serve: %v", err)
        }
    }()



	// Test Produce, sending message to store ....

	logger.Printf("Start calling Produce")

	// response, error := broker.Produce(context.Background(),  &proto.ProduceRequest{
	// 	TopicName: "topic1",
	// 	ProducerId: "producer1",
	// 	PartitionId: int32(1),
	// 	Messages: []*proto.ProducerMessage{
    //         {Key: "key1", Value: "value1"},
    //     },
	// })

	// if err != nil {
	// 	logrus.Fatalf("broker produce error: ", error)
	// }

	// logger.Printf("Received Message Topic: %s", response.GetTopicName())
	// logger.Printf("Received Message Topic: %d", response.GetBaseOffset())


	// topicPartition, _ := broker.Data.LoadOrStore("topic1-1", make([]*proto.ConsumerMessage, 0))
	// topicPartitionData := topicPartition.([]*proto.ConsumerMessage)
	// for _, msg := range topicPartitionData {
    //     logger.Printf("Message Offset: %d, Timestamp: %d, Key: %s, Value: %s", msg.GetOffset(), msg.GetTimestamp(), msg.GetKey(), msg.GetValue())
    // }

	   // Number of concurrent producers
	   numProducers := 10
	   var wg sync.WaitGroup
   
	   // Start concurrent producers
	   for i := 0; i < numProducers; i++ {
		   wg.Add(1)
		   go func(producerID int) {
			   defer wg.Done()
			   for j := 0; j < 101; j++ {
				   response, err := broker.Produce(context.Background(), &proto.ProduceRequest{
					   TopicName:  "topic1",
					   ProducerId: fmt.Sprintf("producer%d", producerID),
					   PartitionId: int32(1),
					   Messages: []*proto.ProducerMessage{
						   {Key: fmt.Sprintf("key%d-%d", producerID, j), Value: fmt.Sprintf("value%d-%d", producerID, j)},
					   },
				   })
   
				   if err != nil {
					   logrus.Fatalf("broker produce error: %v", err)
				   }
   
				   logger.Printf("Produced Message %d by Producer %d: Topic: %s, BaseOffset: %d", j, producerID, response.GetTopicName(), response.GetBaseOffset())
			   }
		   }(i)
	   }
   
	   // Wait for all producers to finish
	   wg.Wait()


   
   
	   // Log the MessageCount for topic1-1
	//    broker.messageCountMu.Lock()
	//    messageCount := broker.MessageCount["topic1-1"]
	//    broker.messageCountMu.Unlock()
	//    logger.Printf("MessageCount for topic1-1: %d", messageCount)


	// ------------ create binary file for each message ----------
	
	// dirPath := "topic1-1"
	// err = os.MkdirAll(dirPath, 0755)
	// if err != nil {
	// 	logrus.Fatalf("Failed to create directory: %v", err)
	// 	return
	// }



	// for _, msg := range topicPartitionData {
	// 	dataBytes, err := proto1.Marshal(msg)
	// 	if err != nil {
	// 		logrus.Fatalf("Failed to marshal message: %v", err)
	// 		continue
	// 	}
	
	// 	// Use a unique filename, for example, by offset:
	// 	// If offset is unique for each message, this works well.
	// 	// Otherwise, consider a timestamp or a UUID.
	// 	fileName := fmt.Sprintf("message-%d.bin", msg.GetOffset())
	// 	filePath := filepath.Join(dirPath, fileName)
	
	// 	err = os.WriteFile(filePath, dataBytes, 0644)
	// 	if err != nil {
	// 		logrus.Fatalf("Failed to write file %s: %v", filePath, err)
	// 	}
	
	// 	logger.Printf("Wrote message with offset %d to %s", msg.GetOffset(), filePath)
	// }


	// // read the file
	// storedBytes, err := os.ReadFile(filepath.Join(dirPath, "message-0.bin"))
	// if err != nil {
	// 	logger.Printf("Failed to read file: %v", err)
	// 	return
	// }

	// retrievedMsg := &proto.ConsumerMessage{}
	// err = proto1.Unmarshal(storedBytes, retrievedMsg)
	// if err != nil {
	// 	logger.Printf("Failed to unmarshal message: %v", err)
	// 	return
	// }
	// logger.Printf("Retrieved Message Offset: %d, Key: %s, Value: %s", retrievedMsg.GetOffset(), retrievedMsg.GetKey(), retrievedMsg.GetValue())

	
    

	// - - - - - - - - - - - - - - - - - - - - - - - -  ConsumerOffset  - - - - - - - - - - - - - - - - - - - - - - 

	// if len(broker.ConsumerOffset) == 0 {
	// 	logger.Printf("Consumer Offset size: %d", len(broker.ConsumerOffset))
	// }else{

	// 	for consumerID, offsets := range broker.ConsumerOffset {
	// 		for partitionID, offset := range offsets {
	// 			logger.Printf("ConsumerID: %s, PartitionID: %s, Offset: %d", consumerID, partitionID, offset)
	// 		}
	// 	}

	// }

	// broker.Consume(context.Background(), &proto.ConsumeRequest{
	// 	ConsumerId: "consumer1",
	// 	TopicName: "topic1",
	// 	PartitionId: int32(1),
	// })

	// if len(broker.ConsumerOffset) == 0 {
	// 	logger.Printf("Consumer Offset size: %d", len(broker.ConsumerOffset))
	// }else{
	// 	for consumerID, offsets := range broker.ConsumerOffset {
	// 		for partitionID, offset := range offsets {
	// 			logger.Printf("ConsumerID: %s, PartitionID: %s, Offset: %d", consumerID, partitionID, offset)
	// 		}
	// 	}

	// }

	// - - - - - - - - - - - - - - - - - - - - - - - - 


	// go broker.SendHeartbeat()
	
}
