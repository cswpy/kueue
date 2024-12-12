package test

import (
	"context"
	"kueue/kueue"
	"kueue/kueue/proto"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

func makeBroker(t *testing.T) *kueue.Broker {
	bi := &kueue.BrokerInfo{
		BrokerName:   "broker1",
		NodeAddr:     "127.0.0.1:8001",
		HostedTopics: make(map[string]*kueue.TopicInfo),
	}
	b, err := kueue.NewBroker(bi, "127.0.0.1:8000", *logrus.WithFields(logrus.Fields{}))
	assert.NoError(t, err)
	return b
}

func TestBroker(t *testing.T) {
	lis := bufconn.Listen(1024 * 1024)

	s := grpc.NewServer()
	defer s.Stop()
	b := makeBroker(t)

	proto.RegisterBrokerServiceServer(s, b)

	go func() {
		if err := s.Serve(lis); err != nil {
			panic(err)
		}
	}()

	// See https://stackoverflow.com/questions/78485578/how-to-use-the-bufconn-package-with-grpc-newclient
	conn, err := grpc.NewClient("passthrough://buffnet", grpc.WithDefaultCallOptions())

	assert.NoError(t, err)

	client := proto.NewBrokerServiceClient(conn)

	client.ProduceMessage(context.Background(), &proto.ProduceRequest{
		TopicName: "topic1",
		ProducerId: "producer1",
		Message: &proto.Message{
	})

}
