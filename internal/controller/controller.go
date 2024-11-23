package controller

import (
	"fmt"
	kueued "kueue/internal"
	"kueue/internal/logging"
	_ "kueue/internal/logging"
	"sync"

	"github.com/sirupsen/logrus"
)

// Controller manages metadata for the MQ cluster.
type Controller struct {
	mu       sync.RWMutex // Ensures thread-safe access to metadata
	Metadata *Metadata    // Stores cluster metadata
}

// NewController initializes a new Controller.
func NewController(controllerID string) *Controller {
	return &Controller{
		Metadata: &Metadata{
			BrokerInfos:  make(map[string]*kueued.BrokerInfo),
			TopicInfos:   make(map[string]*kueued.TopicInfo),
			ControllerID: controllerID,
		},
	}
}

// RegisterBroker registers a new broker with the controller.
func (c *Controller) RegisterBroker(brokerName, address string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the broker already exists
	if _, exists := c.Metadata.BrokerInfos[brokerName]; exists {
		logrus.WithField("Topic", logging.DController).Warnf("Broker %s is already registered.", brokerName)
		return nil
	}

	// Add the broker to the metadata
	c.Metadata.BrokerInfos[brokerName] = &kueued.BrokerInfo{
		BrokerName: brokerName,
		NodeInfo: kueued.NodeInfo{
			Address: address,
			Port:    9092,
		},
		HostedTopics:     make(map[string]*kueued.TopicInfo),
		HostedPartitions: make(map[string]*kueued.PartitionInfo),
	}
	logrus.WithField("Topic", logging.DController).Infof("Broker %s registered at %s.", brokerName, address)
	return nil
}

// CreateTopic creates a new topic and assigns partitions to brokers.
func (c *Controller) CreateTopic(topicName string, partitionCount int, replicationFactor int) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if the topic already exists
	if _, exists := c.Metadata.TopicInfos[topicName]; exists {
		err := fmt.Errorf("Topic %s already exists.", topicName)
		logrus.WithField("Topic", logging.DController).Warnf(err.Error())
		return err
	}

	// Create the topic and partitions
	topic := &kueued.TopicInfo{
		TopicName:         topicName,
		TopicPartitions:   make(map[int]*kueued.PartitionInfo, partitionCount),
		ReplicationFactor: replicationFactor,
		LeaderPartitionID: -1,
	}

	// Assign partitions to brokers
	brokerIDs := c.getActiveBrokerIDs()
	if len(brokerIDs) == 0 {
		err := fmt.Errorf("No active brokers available to assign partitions for topic %s", topicName)
		logrus.WithField("Topic", logging.DController).Errorf(err.Error())
		return err
	}

	for i := 0; i < partitionCount; i++ {
		leaderBroker := brokerIDs[i%len(brokerIDs)]
		//replicas := c.selectReplicas(brokerIDs, leaderBroker, replicationFactor)
		partitionID := fmt.Sprintf("%s-%d", topicName, i)
		partition := &kueued.PartitionInfo{
			PartitionID: partitionID,
			IsLeader:    true,
		}

		// Assign partition to topic
		topic.TopicPartitions[i] = partition

		// Update brokerInfo
		c.Metadata.BrokerInfos[leaderBroker].HostedPartitions[partitionID] = partition
		c.Metadata.BrokerInfos[leaderBroker].HostedTopics[topicName] = topic
	}

	// Add the topic to metadata
	c.Metadata.TopicInfos[topicName] = topic
	logrus.Infof("Topic %s with %d partitions created.", topicName, partitionCount)
	return nil
}

// GetMetadata returns the current cluster metadata.
func (c *Controller) GetMetadata() *Metadata {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Metadata
}

// getActiveBrokerIDs returns a list of active broker IDs.
func (c *Controller) getActiveBrokerIDs() []string {
	brokerIDs := []string{}
	for id, broker := range c.Metadata.BrokerInfos {
		if broker.IsHealthy {
			brokerIDs = append(brokerIDs, id)
		}
	}
	return brokerIDs
}

// selectReplicas selects brokers for replicas, ensuring the leader is included.
func (c *Controller) selectReplicas(brokerIDs []string, leader string, replicationFactor int) []string {
	replicas := []string{leader} // Leader is always the first replica
	for _, brokerID := range brokerIDs {
		if brokerID != leader && len(replicas) < replicationFactor {
			replicas = append(replicas, brokerID)
		}
	}
	return replicas
}
