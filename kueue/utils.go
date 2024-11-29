package kueue

import "fmt"

func (ni *NodeInfo) FullAddress() string {
	return fmt.Sprintf("%s:%d", ni.Address, ni.Port)
}

// func makeConnection(addr string) (proto.Contr, error) {
// 	var opts []grpc.DialOption
// 	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

// 	channel, err := grpc.Dial(addr, opts...)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return proto.NewKvClient(channel), nil
// }
