// All methods in this file are not thread-safe, they are meant to be called from
// controller.go with the lock held.

package kueue

import (
	"fmt"
	"kueue/kueue/proto"

	_ "github.com/sirupsen/logrus"
)

// Metadata associates broker nodes with topic-partitions.
// TopicInfos: this is where all topic-partitions related data resides
// BrokerInfos: a map from brokerName to brokerInfo
type Metadata struct {
	BrokerInfos map[string]*BrokerInfo // broker id to brokerInfo, only read data from BrokerInfo.HostedTopics

	// topic name to TopicInfo, then to PartitionInfo
	// PartitionInfo has a *BrokerInfo pointer, which points to Metadata.BrokerInfos
	// Modifying Metadata.BrokerInfos will affect the *BrokerInfo pointer stored in Metadata.TopicInfos.PartitionInfo
	TopicInfos   map[string]*TopicInfo //
	ControllerID string
}

// Creates a topic and place its TopicInfo in Metadata.TopicInfos
// it does not add the topic to any broker
// TODO implement replication-related logic
func (m *Metadata) createTopic(topicName string, numPartitions int, replicationFactor int) *TopicInfo {
	ti := &TopicInfo{
		TopicName:         topicName,
		TopicPartitions:   make(map[int]*PartitionInfo),
		ReplicationFactor: replicationFactor,
	}

	for i := 0; i < numPartitions; i++ {
		ti.TopicPartitions[i] = &PartitionInfo{
			PartitionID: i,
			IsLeader:    true,
		}
	}

	m.TopicInfos[topicName] = ti

	return ti
}

func (m *Metadata) assignTopicPartitionToBroker(topicName string, partitionId int, brokerName string) error {

	if _, ok := m.TopicInfos[topicName]; !ok {
		err := fmt.Errorf("topic %s does not exist", topicName)
		return err
	}

	ti := m.TopicInfos[topicName]
	if _, ok := ti.TopicPartitions[partitionId]; !ok {
		err := fmt.Errorf("partition %d does not exist", partitionId)
		return err
	}

	partition := ti.TopicPartitions[partitionId]

	if _, ok := m.BrokerInfos[brokerName]; !ok {
		err := fmt.Errorf("broker %s does not exist", brokerName)
		return err
	}

	partition.OwnerBroker = m.BrokerInfos[brokerName]
	return nil
}

// TODO handle replication
// Caller should check if the topic exists before calling this function
func (m *Metadata) getTopicPartitions(topicName string) []*proto.PartitionMetadata {

	ti := m.TopicInfos[topicName]
	partitionInfoArr := make([]*proto.PartitionMetadata, 0)
	for _, partition := range ti.TopicPartitions {
		partitionInfoArr = append(partitionInfoArr, &proto.PartitionMetadata{
			PartitionId:   int32(partition.PartitionID),
			LeaderAddress: partition.OwnerBroker.NodeAddr,
		})
	}

	return partitionInfoArr
}
