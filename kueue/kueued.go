package kueue

import "kueue/kueue/proto"

type BrokerInfo struct {
	BrokerName string // unique name of the broker
	NodeAddr   string
}

func (bi *BrokerInfo) getProto() *proto.BrokerInfoProto {
	return &proto.BrokerInfoProto{
		BrokerId: bi.BrokerName,
		Addr:     bi.NodeAddr,
	}
}

type PartitionInfo struct {
	PartitionID       int         // unique id of the partition
	LeaderBroker      *BrokerInfo // the broker that owns this partition
	ReplicaBrokers    []*BrokerInfo
	AssignedConsumers map[string]struct{} // a set of consumer names
}

func (pi *PartitionInfo) getProto() *proto.PartitionInfo {
	var replicas []*proto.BrokerInfoProto
	for _, replica := range pi.ReplicaBrokers {
		replicas = append(replicas, replica.getProto())
	}

	return &proto.PartitionInfo{
		PartitionId: int32(pi.PartitionID),
		Leader:      pi.LeaderBroker.getProto(),
		Replicas:    replicas,
	}
}

type TopicInfo struct {
	TopicName         string                 // unique name of the topic
	Partitions        map[int]*PartitionInfo // partition id to partition info
	ReplicationFactor int                    // replication factor of each partition
}

// Record represents a message record in the message queue
// reference: https://kafka.apache.org/documentation/#record
type Record struct {
	Offset     int64
	CreateTime int64 // unix timestamp in milliseconds
	Key        string
	Value      string
}
