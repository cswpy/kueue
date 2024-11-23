package controller

import (
	kueued "kueue/internal"

	_ "github.com/sirupsen/logrus"
)

type Metadata struct {
	BrokerInfos  map[string]*kueued.BrokerInfo // broker id to broker
	TopicInfos   map[string]*kueued.TopicInfo  // topic name to topic
	ControllerID string
}
