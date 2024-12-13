package kueue

import (
	"context"
	"kueue/kueue/proto"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestBrokerProduceConsume(t *testing.T) {

	b := NewMockBroker(*logrus.WithField("test", "broker"))

	msgs := []*proto.ProducerMessage{
		{Key: "key1", Value: "value1"},
		{Key: "key2", Value: "value2"},
		{Key: "key3", Value: "value3"},
		{Key: "key4", Value: "value4"},
		{Key: "key5", Value: "value5"},
	}

	produceRequest := proto.ProduceRequest{
		TopicName:   "topic1",
		ProducerId:  "producer1",
		PartitionId: 0,
		Messages:    msgs,
	}

	resp1, err := b.Produce(context.Background(), &produceRequest)
	assert.NoError(t, err)
	assert.EqualValues(t, resp1.BaseOffset, 0)

	consumeRequest := proto.ConsumeRequest{
		TopicName:   "topic1",
		PartitionId: 0,
		ConsumerId:  "consumer1",
	}

	resp2, err := b.Consume(context.Background(), &consumeRequest)
	assert.NoError(t, err)
	assert.Equal(t, resp2.TopicName, "topic1")
	assert.Len(t, resp2.Records, len(msgs))
	for i := 0; i < len(msgs); i++ {
		assert.Equal(t, resp2.Records[i].Key, msgs[i].Key)
		assert.Equal(t, resp2.Records[i].Value, msgs[i].Value)
	}
}

// func TestBroker(t *testing.T) {
// 	lis := bufconn.Listen(1024 * 1024)

// 	s := grpc.NewServer()
// 	defer s.Stop()
// 	b := makeBroker(t)

// 	proto.RegisterBrokerServiceServer(s, b)

// 	go func() {
// 		if err := s.Serve(lis); err != nil {
// 			panic(err)
// 		}
// 	}()

// 	// See https://stackoverflow.com/questions/78485578/how-to-use-the-bufconn-package-with-grpc-newclient
// 	conn, err := grpc.NewClient("passthrough://buffnet", grpc.WithDefaultCallOptions())

// 	assert.NoError(t, err)

// 	client := proto.NewBrokerServiceClient(conn)

// 	// client.ProduceMessage(context.Background(), &proto.ProduceRequest{
// 	// 	TopicName: "topic1",
// 	// 	ProducerId: "producer1",
// 	// 	Message: &proto.Message{
// 	// })

// }
