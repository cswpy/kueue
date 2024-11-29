package main

import (
	"flag"
	"kueue/kueue"
	"net"

	proto "kueue/kueue/proto"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
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
)

func main() {
	flag.Parse()
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
	logrus.WithField("Topic", kueue.DBroker).Infof("Broker service listening on %s", *brokerServiceAddr)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	bi := &kueue.BrokerInfo{
		BrokerName:       *brokerName,
		NodeAddr:         *brokerServiceAddr,
		HostedTopics:     make(map[string]*kueue.TopicInfo),
		HostedPartitions: make(map[string]*kueue.PartitionInfo),
	}

	broker, err := kueue.NewBroker(bi, *controllerAddr)

	if err != nil {
		logrus.Fatalf("Failed to create broker: %v", err)
	}

	proto.RegisterBrokerServiceServer(grpcServer, broker)
	grpcServer.Serve(lis)
}
