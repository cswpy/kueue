package kueue

import (
	"context"
	"kueue/kueue/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Broker struct {
	proto.UnimplementedBrokerServiceServer
	Info           *BrokerInfo
	ControllerInfo string
	conn           *grpc.ClientConn
}

func MakeBroker(info *BrokerInfo, controllerInfo string) (*Broker, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(info.NodeAddr, opts...)
	if err != nil {
		return nil, err
	}

	return &Broker{
		Info:           info,
		ControllerInfo: controllerInfo,
		conn:           conn,
	}, nil
}

func (b *Broker) sendHeartbeat() {
	client := proto.NewBrokerServiceClient(b.conn)

	client.Heartbeat(context.Background(), &proto.HeartbeatRequest{
		BrokerId: b.Info.BrokerName,
	})
}
