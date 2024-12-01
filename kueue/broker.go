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
	Info           *BrokerInfo
	ControllerAddr string
	client         proto.ControllerServiceClient
}

func NewBroker(info *BrokerInfo, controllerAddr string) (*Broker, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(controllerAddr, opts...)
	if err != nil {
		return nil, err
	}
	fmt.Printf("Connected to controller at %s\n", controllerAddr)
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
		Info:           info,
		ControllerAddr: controllerAddr,
		client:         client,
	}, nil
}

func (b *Broker) SendHeartbeat() {

	for {
		logrus.WithField("Topic", DBroker).Infof("Sending heartbeat to controller.")
		_, err := b.client.Heartbeat(context.Background(), &proto.HeartbeatRequest{
			BrokerId: b.Info.BrokerName,
		})
		if err != nil {
			logrus.WithField("Topic", DBroker).Errorf("Error sending heartbeat: %v", err)
		}
		time.Sleep(5 * time.Second)
	}
}
