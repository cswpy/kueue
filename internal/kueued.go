package internal

type BrokerInfo struct {
	BrokerName       string // unique name of the broker
	NodeInfo         NodeInfo
	IsHealthy        bool
	HostedTopics     map[string]*TopicInfo     // topic name to topic
	HostedPartitions map[string]*PartitionInfo // partition id to partition
}

type NodeInfo struct {
	Address string `json:"address"`
	Port    int32  `json:"port"`
}

type PartitionInfo struct {
	PartitionID string // unique id of the partition
	IsLeader    bool
}

type TopicInfo struct {
	TopicName         string // unique name of the topic
	TopicPartitions   map[int]*PartitionInfo
	ReplicationFactor int // replication factor of each partition
	LeaderPartitionID int // unique id of the leader partition
}

// Record represents a message record in the message queue
// reference: https://kafka.apache.org/documentation/#record
type Record struct {
	Offset     int64
	CreateTime int64 // unix timestamp in milliseconds
	Key        string
	Value      string
}
