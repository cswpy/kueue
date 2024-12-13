package kueue

type BrokerInfo struct {
	BrokerName   string // unique name of the broker
	NodeAddr     string
	HostedTopics map[string]*TopicInfo // topic name to topic
	PersistBatch int
}

type PartitionInfo struct {
	PartitionID int // unique id of the partition
	IsLeader    bool
}

type TopicInfo struct {
	TopicName         string // unique name of the topic
	TopicPartitions   map[int]*PartitionInfo
	ReplicationFactor int // replication factor of each partition
}

// Record represents a message record in the message queue
// reference: https://kafka.apache.org/documentation/#record
type Record struct {
	Offset     int64
	CreateTime int64 // unix timestamp in milliseconds
	Key        string
	Value      string
}
