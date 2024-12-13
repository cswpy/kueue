package kueue

import (
	"context"
	"fmt"
	proto "kueue/kueue/proto"
	"slices"
	"sync"
	"time"

	"maps"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Controller manages metadata for the MQ cluster.
type Controller struct {
	proto.UnimplementedControllerServiceServer
	mu           sync.RWMutex // Ensures thread-safe access to metadata
	Metadata     *Metadata    // Stores cluster metadata, has brokerInfos and topicInfos
	BrokerStatus map[string]time.Time
	logger       logrus.Entry
}

// NewController initializes a new Controller.
func NewController(controllerID string, logger logrus.Entry) *Controller {
	return &Controller{
		Metadata: &Metadata{
			BrokerInfos:  make(map[string]*BrokerInfo),
			TopicInfos:   make(map[string]*TopicInfo),
			ControllerID: controllerID,
		},
		BrokerStatus: make(map[string]time.Time),
		logger:       logger,
	}
}

// RegisterBroker registers a new broker with the controller.
func (c *Controller) RegisterBroker(ctx context.Context, req *proto.RegisterBrokerRequest) (*proto.RegisterBrokerResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	brokerName := req.BrokerId
	brokerAddr := req.BrokerAddress
	// Check if the broker already exists
	if _, exists := c.Metadata.BrokerInfos[brokerName]; exists {
		warn := fmt.Sprintf("Broker %s is already registered", brokerName)
		c.logger.WithField("Topic", DController).Warnf(warn)
		return nil, status.Error(codes.AlreadyExists, warn)
	}

	// Add the broker to the metadata
	c.Metadata.BrokerInfos[brokerName] = &BrokerInfo{
		BrokerName: brokerName,
		NodeAddr:   brokerAddr,
	}
	c.BrokerStatus[brokerName] = time.Now()
	c.logger.WithField("Topic", DController).Infof("Broker %s registered at %s.", brokerName, brokerAddr)
	return &proto.RegisterBrokerResponse{}, nil
}

// GetTopic creates a new topic and assigns partitions to brokers if not exists, and return the partition metadata to producer
func (c *Controller) GetTopic(ctx context.Context, req *proto.ProducerTopicRequest) (*proto.ProducerTopicResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Return the partition metadata if the topic already exists
	if _, exists := c.Metadata.TopicInfos[req.TopicName]; exists {
		arr := c.Metadata.getTopicPartitions(req.TopicName)
		resp := &proto.ProducerTopicResponse{
			TopicName:  req.TopicName,
			Partitions: arr,
		}
		return resp, nil
	}

	// Get healthy brokers first, if no brokers are available, return an error
	brokerIDs := c.getActiveBrokerIDs()

	if len(brokerIDs) == 0 {
		err := fmt.Errorf("no active brokers available to assign partitions for topic %s", req.TopicName)
		c.logger.WithField("Topic", DController).Errorf(err.Error())
		return nil, status.Error(codes.Internal, err.Error())
	}

	c.Metadata.createTopic(req.TopicName, int(*req.NumPartitions), int(*req.ReplicationFactor))

	// TODO - select brokers for replicas
	// Assign partitions to brokers, wrap around if needed
	for idx := 0; idx < int(*req.NumPartitions); idx++ {
		c.Metadata.assignTopicPartitionToBroker(req.TopicName, idx, brokerIDs[idx%len(brokerIDs)])
	}

	c.logger.Infof("Topic %s with %d partitions created.", req.TopicName, *req.NumPartitions)

	partitionsArr := c.Metadata.getTopicPartitions(req.TopicName)

	resp := &proto.ProducerTopicResponse{
		TopicName:  req.TopicName,
		Partitions: partitionsArr,
	}
	return resp, nil
}

// GetMetadata returns the current cluster metadata.
func (c *Controller) GetMetadata() *Metadata {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.Metadata
}

// Heartbeat updates the broker's health status.
func (c *Controller) Heartbeat(ctx context.Context, req *proto.HeartbeatRequest) (*proto.HeartbeatResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logger.WithField("Topic", DController).Infof("Received heartbeat from broker %s.", req.BrokerId)
	// Update broker health status
	brokerName := req.BrokerId
	if _, exists := c.Metadata.BrokerInfos[brokerName]; exists {
		c.BrokerStatus[brokerName] = time.Now()
		c.logger.WithField("Topic", DController).Infof("Broker %s is healthy.", brokerName)
	} else {
		err := fmt.Errorf("broker %s not found", brokerName)
		c.logger.WithField("Topic", DController).Errorf(err.Error())
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	resp := &proto.HeartbeatResponse{}
	return resp, nil
}

// getActiveBrokerIDs returns a list of active broker IDs, not thread-safe
func (c *Controller) getActiveBrokerIDs() []string {
	return slices.Collect(maps.Keys(c.BrokerStatus))
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
