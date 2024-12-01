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
	controllerServiceAddr = flag.String(
		"controller-address",
		"127.0.0.1:8080",
		"host ip address of the broker service in the format of host:port",
	)
	controllerID = flag.String(
		"controller-id",
		"CTRL-1",
		"unique name of the controller",
	)
)

func main() {
	flag.Parse()

	lis, err := net.Listen("tcp", *controllerServiceAddr)
	if err != nil {
		logrus.Fatalf("Failed to listen: %v", err)
	}

	logger := logrus.WithField("Node", *controllerID)

	logger.WithField("Topic", kueue.DBroker).Infof("Controller service listening on %s", *controllerServiceAddr)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	controller := kueue.NewController(*controllerID, *logger)

	proto.RegisterControllerServiceServer(grpcServer, controller)
	grpcServer.Serve(lis)
}
