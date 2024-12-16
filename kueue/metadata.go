// All methods in this file are not thread-safe, they are meant to be called from
// controller.go with the lock held.

package kueue

import (
	"fmt"
	"maps"
	"slices"

	_ "github.com/sirupsen/logrus"
)

// Metadata associates broker nodes with topic-partitions.
// TopicInfos: this is where all topic-partitions related data resides
// BrokerInfos: a map from brokerName to brokerInfo
type Metadata struct {
	BrokerInfos map[string]*BrokerInfo // broker id to brokerInfo

	// topic name to TopicInfo, then to PartitionInfo
	// PartitionInfo has a *BrokerInfo pointer, which points to Metadata.BrokerInfos
	// Modifying Metadata.BrokerInfos will affect the *BrokerInfo pointer stored in Metadata.TopicInfos.PartitionInfo
	TopicInfos   map[string]*TopicInfo
	ControllerID string
}

func MakeNewMetadata(controllerID string) *Metadata {
	return &Metadata{
		BrokerInfos:  make(map[string]*BrokerInfo),
		TopicInfos:   make(map[string]*TopicInfo),
		ControllerID: controllerID,
	}
}

// Creates a topic and place its TopicInfo in Metadata.TopicInfos
// it does not add the topic to any broker
// TODO implement replication-related logic
func (m *Metadata) createTopic(topicName string, numPartitions int, replicationFactor int) *TopicInfo {
	ti := &TopicInfo{
		TopicName:         topicName,
		Partitions:        make(map[int]*PartitionInfo),
		ReplicationFactor: replicationFactor,
	}

	for i := 0; i < numPartitions; i++ {
		ti.Partitions[i] = &PartitionInfo{
			PartitionID:       i,
			LeaderBroker:      nil,
			ReplicaBrokers:    make([]*BrokerInfo, 0),
			AssignedConsumers: make(map[string]struct{}),
		}
	}

	m.TopicInfos[topicName] = ti

	return ti
}

func (m *Metadata) assignPartitionReplica(topicName string, partitionId int, replicaSet []string) error {
	var ok bool
	var ti *TopicInfo
	if ti, ok = m.TopicInfos[topicName]; !ok {
		err := fmt.Errorf("topic %s does not exist", topicName)
		return err
	}

	var partition *PartitionInfo
	if partition, ok = ti.Partitions[partitionId]; !ok {
		err := fmt.Errorf("partition %d does not exist", partitionId)
		return err
	}

	leader := replicaSet[0]
	var leaderBroker *BrokerInfo

	// Assign leader broker
	if leaderBroker, ok = m.BrokerInfos[leader]; !ok {
		err := fmt.Errorf("partition leader broker %s does not exist", leader)
		return err
	}
	partition.LeaderBroker = leaderBroker

	// Assign replicas
	var replicaBroker *BrokerInfo
	for idx := 1; idx < len(replicaSet); idx++ {
		replica := replicaSet[idx]

		if replicaBroker, ok = m.BrokerInfos[replica]; !ok {
			err := fmt.Errorf("partition replica broker %s does not exist", replica)
			return err
		}
		partition.ReplicaBrokers = append(partition.ReplicaBrokers, replicaBroker)
	}
	return nil
}

// // TODO handle replication
// // Caller should check if the topic exists before calling this function
func (m *Metadata) getPartitions(topicName string) []*PartitionInfo {
	return slices.Collect(maps.Values(m.TopicInfos[topicName].Partitions))
}
