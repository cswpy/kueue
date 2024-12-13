package kueue

type BrokerInfo struct {
	BrokerName string // unique name of the broker
	NodeAddr   string // address of the broker
}

type PartitionInfo struct {
	PartitionID int         // unique id of the partition
	OwnerBroker *BrokerInfo // the broker that owns this partition
	IsLeader    bool
}

type TopicInfo struct {
	TopicName         string                 // unique name of the topic
	TopicPartitions   map[int]*PartitionInfo // partition id to partition info
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
