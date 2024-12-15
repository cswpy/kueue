package kueue

import "kueue/kueue/proto"

type BrokerInfo struct {
	BrokerName string // unique name of the broker
	NodeAddr   string
}

type PartitionInfo struct {
	PartitionID       int         // unique id of the partition
	LeaderBroker      *BrokerInfo // the broker that owns this partition
	ReplicaBrokers    []*BrokerInfo
	AssignedConsumers []string
}

func (pi *PartitionInfo) getProto() *proto.PartitionMetadata {
	return &proto.PartitionMetadata{
		PartitionId:   int32(pi.PartitionID),
		LeaderAddress: pi.OwnerBroker.NodeAddr,
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
