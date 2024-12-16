// This file is adapted from https://git.cs426.cloud/labs/cs426-fall24/src/branch/main/lab4/kv/clientpool.go

package kueue

import (
	"kueue/kueue/proto"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func makeConnection(addr string) (proto.BrokerServiceClient, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))

	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		return nil, err
	}
	client := proto.NewBrokerServiceClient(conn)
	return client, nil
}

/*
 * ClientPool will be used by leader partition broker to call into peer brokers that host the repliacas
 * or used by the controller to call into the leader brokers to appoint them as leaders.
 *
 * Nodes are cached by nodeName. We assume that the connection information (Address/Port)
 * will never change for a given nodeName.
 */
type ClientPool interface {
	/*
	 * Returns a Client for a given node if one can be created. Returns (nil, err)
	 * otherwise. Errors are not cached, so subsequent calls may return a valid KvClient.
	 */
	GetClient(bi *BrokerInfo) (proto.BrokerServiceClient, error)
}

type GrpcClientPool struct {
	mutex   sync.RWMutex
	clients map[string]proto.BrokerServiceClient
}

func MakeClientPool() *GrpcClientPool {
	return &GrpcClientPool{clients: make(map[string]proto.BrokerServiceClient)}
}

func (pool *GrpcClientPool) GetClient(bi *BrokerInfo) (proto.BrokerServiceClient, error) {
	// Optimistic read -- most cases we will have already cached the client, so
	// only take a read lock to maximize concurrency here
	pool.mutex.RLock()
	client, ok := pool.clients[bi.BrokerName]
	pool.mutex.RUnlock()
	if ok {
		return client, nil
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	// We may have lost a race and someone already created a client, try again
	// while holding the exclusive lock
	client, ok = pool.clients[bi.BrokerName]
	if ok {
		return client, nil
	}

	// Otherwise create the client: gRPC expects an address of the form "ip:port"
	client, err := makeConnection(bi.NodeAddr)
	if err != nil {
		return nil, err
	}
	pool.clients[bi.BrokerName] = client
	return client, nil
}
