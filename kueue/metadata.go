package kueue

import (
	_ "github.com/sirupsen/logrus"
)

type Metadata struct {
	BrokerInfos  map[string]*BrokerInfo // broker id to broker
	TopicInfos   map[string]*TopicInfo  // topic name to topic
	ControllerID string
}
