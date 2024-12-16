// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.2
// 	protoc        v5.27.3
// source: kueue/proto/controller.proto

package proto

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// num_partitions and replication_factor are optional when getting an existing topic
type ProducerTopicRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TopicName         string `protobuf:"bytes,1,opt,name=topic_name,json=topicName,proto3" json:"topic_name,omitempty"`
	NumPartitions     *int32 `protobuf:"varint,2,opt,name=num_partitions,json=numPartitions,proto3,oneof" json:"num_partitions,omitempty"`
	ReplicationFactor *int32 `protobuf:"varint,3,opt,name=replication_factor,json=replicationFactor,proto3,oneof" json:"replication_factor,omitempty"`
}

func (x *ProducerTopicRequest) Reset() {
	*x = ProducerTopicRequest{}
	mi := &file_kueue_proto_controller_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ProducerTopicRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProducerTopicRequest) ProtoMessage() {}

func (x *ProducerTopicRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kueue_proto_controller_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProducerTopicRequest.ProtoReflect.Descriptor instead.
func (*ProducerTopicRequest) Descriptor() ([]byte, []int) {
	return file_kueue_proto_controller_proto_rawDescGZIP(), []int{0}
}

func (x *ProducerTopicRequest) GetTopicName() string {
	if x != nil {
		return x.TopicName
	}
	return ""
}

func (x *ProducerTopicRequest) GetNumPartitions() int32 {
	if x != nil && x.NumPartitions != nil {
		return *x.NumPartitions
	}
	return 0
}

func (x *ProducerTopicRequest) GetReplicationFactor() int32 {
	if x != nil && x.ReplicationFactor != nil {
		return *x.ReplicationFactor
	}
	return 0
}

// Controller returns the leader broker info to producer
type ProducerTopicResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TopicName  string           `protobuf:"bytes,1,opt,name=topic_name,json=topicName,proto3" json:"topic_name,omitempty"`
	Partitions []*PartitionInfo `protobuf:"bytes,2,rep,name=partitions,proto3" json:"partitions,omitempty"`
}

func (x *ProducerTopicResponse) Reset() {
	*x = ProducerTopicResponse{}
	mi := &file_kueue_proto_controller_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ProducerTopicResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ProducerTopicResponse) ProtoMessage() {}

func (x *ProducerTopicResponse) ProtoReflect() protoreflect.Message {
	mi := &file_kueue_proto_controller_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ProducerTopicResponse.ProtoReflect.Descriptor instead.
func (*ProducerTopicResponse) Descriptor() ([]byte, []int) {
	return file_kueue_proto_controller_proto_rawDescGZIP(), []int{1}
}

func (x *ProducerTopicResponse) GetTopicName() string {
	if x != nil {
		return x.TopicName
	}
	return ""
}

func (x *ProducerTopicResponse) GetPartitions() []*PartitionInfo {
	if x != nil {
		return x.Partitions
	}
	return nil
}

// When only returning leader, leave replicas empty
type PartitionInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	PartitionId int32              `protobuf:"varint,1,opt,name=partition_id,json=partitionId,proto3" json:"partition_id,omitempty"`
	Leader      *BrokerInfoProto   `protobuf:"bytes,2,opt,name=leader,proto3" json:"leader,omitempty"`
	Replicas    []*BrokerInfoProto `protobuf:"bytes,3,rep,name=replicas,proto3" json:"replicas,omitempty"`
}

func (x *PartitionInfo) Reset() {
	*x = PartitionInfo{}
	mi := &file_kueue_proto_controller_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *PartitionInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PartitionInfo) ProtoMessage() {}

func (x *PartitionInfo) ProtoReflect() protoreflect.Message {
	mi := &file_kueue_proto_controller_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PartitionInfo.ProtoReflect.Descriptor instead.
func (*PartitionInfo) Descriptor() ([]byte, []int) {
	return file_kueue_proto_controller_proto_rawDescGZIP(), []int{2}
}

func (x *PartitionInfo) GetPartitionId() int32 {
	if x != nil {
		return x.PartitionId
	}
	return 0
}

func (x *PartitionInfo) GetLeader() *BrokerInfoProto {
	if x != nil {
		return x.Leader
	}
	return nil
}

func (x *PartitionInfo) GetReplicas() []*BrokerInfoProto {
	if x != nil {
		return x.Replicas
	}
	return nil
}

type BrokerInfoProto struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BrokerId string `protobuf:"bytes,1,opt,name=broker_id,json=brokerId,proto3" json:"broker_id,omitempty"`
	Addr     string `protobuf:"bytes,2,opt,name=addr,proto3" json:"addr,omitempty"`
}

func (x *BrokerInfoProto) Reset() {
	*x = BrokerInfoProto{}
	mi := &file_kueue_proto_controller_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *BrokerInfoProto) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BrokerInfoProto) ProtoMessage() {}

func (x *BrokerInfoProto) ProtoReflect() protoreflect.Message {
	mi := &file_kueue_proto_controller_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BrokerInfoProto.ProtoReflect.Descriptor instead.
func (*BrokerInfoProto) Descriptor() ([]byte, []int) {
	return file_kueue_proto_controller_proto_rawDescGZIP(), []int{3}
}

func (x *BrokerInfoProto) GetBrokerId() string {
	if x != nil {
		return x.BrokerId
	}
	return ""
}

func (x *BrokerInfoProto) GetAddr() string {
	if x != nil {
		return x.Addr
	}
	return ""
}

type SubscribeRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TopicName  string `protobuf:"bytes,1,opt,name=topic_name,json=topicName,proto3" json:"topic_name,omitempty"`
	ConsumerId string `protobuf:"bytes,2,opt,name=consumer_id,json=consumerId,proto3" json:"consumer_id,omitempty"`
}

func (x *SubscribeRequest) Reset() {
	*x = SubscribeRequest{}
	mi := &file_kueue_proto_controller_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SubscribeRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SubscribeRequest) ProtoMessage() {}

func (x *SubscribeRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kueue_proto_controller_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SubscribeRequest.ProtoReflect.Descriptor instead.
func (*SubscribeRequest) Descriptor() ([]byte, []int) {
	return file_kueue_proto_controller_proto_rawDescGZIP(), []int{4}
}

func (x *SubscribeRequest) GetTopicName() string {
	if x != nil {
		return x.TopicName
	}
	return ""
}

func (x *SubscribeRequest) GetConsumerId() string {
	if x != nil {
		return x.ConsumerId
	}
	return ""
}

// Controller returns all broker info, leader and replicas, to consumer
type SubscribeResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	TopicName string         `protobuf:"bytes,1,opt,name=topic_name,json=topicName,proto3" json:"topic_name,omitempty"`
	Partition *PartitionInfo `protobuf:"bytes,2,opt,name=partition,proto3" json:"partition,omitempty"`
}

func (x *SubscribeResponse) Reset() {
	*x = SubscribeResponse{}
	mi := &file_kueue_proto_controller_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *SubscribeResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*SubscribeResponse) ProtoMessage() {}

func (x *SubscribeResponse) ProtoReflect() protoreflect.Message {
	mi := &file_kueue_proto_controller_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use SubscribeResponse.ProtoReflect.Descriptor instead.
func (*SubscribeResponse) Descriptor() ([]byte, []int) {
	return file_kueue_proto_controller_proto_rawDescGZIP(), []int{5}
}

func (x *SubscribeResponse) GetTopicName() string {
	if x != nil {
		return x.TopicName
	}
	return ""
}

func (x *SubscribeResponse) GetPartition() *PartitionInfo {
	if x != nil {
		return x.Partition
	}
	return nil
}

type HeartbeatRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BrokerId string `protobuf:"bytes,1,opt,name=broker_id,json=brokerId,proto3" json:"broker_id,omitempty"`
}

func (x *HeartbeatRequest) Reset() {
	*x = HeartbeatRequest{}
	mi := &file_kueue_proto_controller_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *HeartbeatRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HeartbeatRequest) ProtoMessage() {}

func (x *HeartbeatRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kueue_proto_controller_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HeartbeatRequest.ProtoReflect.Descriptor instead.
func (*HeartbeatRequest) Descriptor() ([]byte, []int) {
	return file_kueue_proto_controller_proto_rawDescGZIP(), []int{6}
}

func (x *HeartbeatRequest) GetBrokerId() string {
	if x != nil {
		return x.BrokerId
	}
	return ""
}

type HeartbeatResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *HeartbeatResponse) Reset() {
	*x = HeartbeatResponse{}
	mi := &file_kueue_proto_controller_proto_msgTypes[7]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *HeartbeatResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*HeartbeatResponse) ProtoMessage() {}

func (x *HeartbeatResponse) ProtoReflect() protoreflect.Message {
	mi := &file_kueue_proto_controller_proto_msgTypes[7]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use HeartbeatResponse.ProtoReflect.Descriptor instead.
func (*HeartbeatResponse) Descriptor() ([]byte, []int) {
	return file_kueue_proto_controller_proto_rawDescGZIP(), []int{7}
}

type RegisterBrokerRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	BrokerId      string `protobuf:"bytes,1,opt,name=broker_id,json=brokerId,proto3" json:"broker_id,omitempty"`
	BrokerAddress string `protobuf:"bytes,2,opt,name=broker_address,json=brokerAddress,proto3" json:"broker_address,omitempty"`
}

func (x *RegisterBrokerRequest) Reset() {
	*x = RegisterBrokerRequest{}
	mi := &file_kueue_proto_controller_proto_msgTypes[8]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RegisterBrokerRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterBrokerRequest) ProtoMessage() {}

func (x *RegisterBrokerRequest) ProtoReflect() protoreflect.Message {
	mi := &file_kueue_proto_controller_proto_msgTypes[8]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RegisterBrokerRequest.ProtoReflect.Descriptor instead.
func (*RegisterBrokerRequest) Descriptor() ([]byte, []int) {
	return file_kueue_proto_controller_proto_rawDescGZIP(), []int{8}
}

func (x *RegisterBrokerRequest) GetBrokerId() string {
	if x != nil {
		return x.BrokerId
	}
	return ""
}

func (x *RegisterBrokerRequest) GetBrokerAddress() string {
	if x != nil {
		return x.BrokerAddress
	}
	return ""
}

type RegisterBrokerResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields
}

func (x *RegisterBrokerResponse) Reset() {
	*x = RegisterBrokerResponse{}
	mi := &file_kueue_proto_controller_proto_msgTypes[9]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *RegisterBrokerResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RegisterBrokerResponse) ProtoMessage() {}

func (x *RegisterBrokerResponse) ProtoReflect() protoreflect.Message {
	mi := &file_kueue_proto_controller_proto_msgTypes[9]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RegisterBrokerResponse.ProtoReflect.Descriptor instead.
func (*RegisterBrokerResponse) Descriptor() ([]byte, []int) {
	return file_kueue_proto_controller_proto_rawDescGZIP(), []int{9}
}

var File_kueue_proto_controller_proto protoreflect.FileDescriptor

var file_kueue_proto_controller_proto_rawDesc = []byte{
	0x0a, 0x1c, 0x6b, 0x75, 0x65, 0x75, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x63, 0x6f,
	0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xbf,
	0x01, 0x0a, 0x14, 0x50, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x54, 0x6f, 0x70, 0x69, 0x63,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x6f, 0x70, 0x69, 0x63,
	0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x6f, 0x70,
	0x69, 0x63, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x2a, 0x0a, 0x0e, 0x6e, 0x75, 0x6d, 0x5f, 0x70, 0x61,
	0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x48, 0x00,
	0x52, 0x0d, 0x6e, 0x75, 0x6d, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x88,
	0x01, 0x01, 0x12, 0x32, 0x0a, 0x12, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x5f, 0x66, 0x61, 0x63, 0x74, 0x6f, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x05, 0x48, 0x01,
	0x52, 0x11, 0x72, 0x65, 0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x46, 0x61, 0x63,
	0x74, 0x6f, 0x72, 0x88, 0x01, 0x01, 0x42, 0x11, 0x0a, 0x0f, 0x5f, 0x6e, 0x75, 0x6d, 0x5f, 0x70,
	0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x42, 0x15, 0x0a, 0x13, 0x5f, 0x72, 0x65,
	0x70, 0x6c, 0x69, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x66, 0x61, 0x63, 0x74, 0x6f, 0x72,
	0x22, 0x66, 0x0a, 0x15, 0x50, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x54, 0x6f, 0x70, 0x69,
	0x63, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x6f, 0x70,
	0x69, 0x63, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74,
	0x6f, 0x70, 0x69, 0x63, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x2e, 0x0a, 0x0a, 0x70, 0x61, 0x72, 0x74,
	0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x50,
	0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x0a, 0x70, 0x61,
	0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x22, 0x8a, 0x01, 0x0a, 0x0d, 0x50, 0x61, 0x72,
	0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x21, 0x0a, 0x0c, 0x70, 0x61,
	0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x05,
	0x52, 0x0b, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x49, 0x64, 0x12, 0x28, 0x0a,
	0x06, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e,
	0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x49, 0x6e, 0x66, 0x6f, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52,
	0x06, 0x6c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x2c, 0x0a, 0x08, 0x72, 0x65, 0x70, 0x6c, 0x69,
	0x63, 0x61, 0x73, 0x18, 0x03, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x42, 0x72, 0x6f, 0x6b,
	0x65, 0x72, 0x49, 0x6e, 0x66, 0x6f, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x52, 0x08, 0x72, 0x65, 0x70,
	0x6c, 0x69, 0x63, 0x61, 0x73, 0x22, 0x42, 0x0a, 0x0f, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x49,
	0x6e, 0x66, 0x6f, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x1b, 0x0a, 0x09, 0x62, 0x72, 0x6f, 0x6b,
	0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x62, 0x72, 0x6f,
	0x6b, 0x65, 0x72, 0x49, 0x64, 0x12, 0x12, 0x0a, 0x04, 0x61, 0x64, 0x64, 0x72, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x04, 0x61, 0x64, 0x64, 0x72, 0x22, 0x52, 0x0a, 0x10, 0x53, 0x75, 0x62,
	0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1d, 0x0a,
	0x0a, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x5f, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x09, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x4e, 0x61, 0x6d, 0x65, 0x12, 0x1f, 0x0a, 0x0b,
	0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x0a, 0x63, 0x6f, 0x6e, 0x73, 0x75, 0x6d, 0x65, 0x72, 0x49, 0x64, 0x22, 0x60, 0x0a,
	0x11, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
	0x73, 0x65, 0x12, 0x1d, 0x0a, 0x0a, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x5f, 0x6e, 0x61, 0x6d, 0x65,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x09, 0x74, 0x6f, 0x70, 0x69, 0x63, 0x4e, 0x61, 0x6d,
	0x65, 0x12, 0x2c, 0x0a, 0x09, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e,
	0x49, 0x6e, 0x66, 0x6f, 0x52, 0x09, 0x70, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e, 0x22,
	0x2f, 0x0a, 0x10, 0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61, 0x74, 0x52, 0x65, 0x71, 0x75,
	0x65, 0x73, 0x74, 0x12, 0x1b, 0x0a, 0x09, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x5f, 0x69, 0x64,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x49, 0x64,
	0x22, 0x13, 0x0a, 0x11, 0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61, 0x74, 0x52, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x5b, 0x0a, 0x15, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65,
	0x72, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1b,
	0x0a, 0x09, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x08, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x49, 0x64, 0x12, 0x25, 0x0a, 0x0e, 0x62,
	0x72, 0x6f, 0x6b, 0x65, 0x72, 0x5f, 0x61, 0x64, 0x64, 0x72, 0x65, 0x73, 0x73, 0x18, 0x02, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x0d, 0x62, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x41, 0x64, 0x64, 0x72, 0x65,
	0x73, 0x73, 0x22, 0x18, 0x0a, 0x16, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x42, 0x72,
	0x6f, 0x6b, 0x65, 0x72, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x32, 0xf9, 0x01, 0x0a,
	0x11, 0x43, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x6c, 0x65, 0x72, 0x53, 0x65, 0x72, 0x76, 0x69,
	0x63, 0x65, 0x12, 0x39, 0x0a, 0x08, 0x47, 0x65, 0x74, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x12, 0x15,
	0x2e, 0x50, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72, 0x54, 0x6f, 0x70, 0x69, 0x63, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x16, 0x2e, 0x50, 0x72, 0x6f, 0x64, 0x75, 0x63, 0x65, 0x72,
	0x54, 0x6f, 0x70, 0x69, 0x63, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x41, 0x0a,
	0x0e, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x12,
	0x16, 0x2e, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74, 0x65, 0x72, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72,
	0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x17, 0x2e, 0x52, 0x65, 0x67, 0x69, 0x73, 0x74,
	0x65, 0x72, 0x42, 0x72, 0x6f, 0x6b, 0x65, 0x72, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
	0x12, 0x32, 0x0a, 0x09, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x12, 0x11, 0x2e,
	0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x1a, 0x12, 0x2e, 0x53, 0x75, 0x62, 0x73, 0x63, 0x72, 0x69, 0x62, 0x65, 0x52, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x12, 0x32, 0x0a, 0x09, 0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61,
	0x74, 0x12, 0x11, 0x2e, 0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61, 0x74, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x1a, 0x12, 0x2e, 0x48, 0x65, 0x61, 0x72, 0x74, 0x62, 0x65, 0x61, 0x74,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x42, 0x13, 0x5a, 0x11, 0x6b, 0x75, 0x65, 0x75,
	0x65, 0x2f, 0x6b, 0x75, 0x65, 0x75, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_kueue_proto_controller_proto_rawDescOnce sync.Once
	file_kueue_proto_controller_proto_rawDescData = file_kueue_proto_controller_proto_rawDesc
)

func file_kueue_proto_controller_proto_rawDescGZIP() []byte {
	file_kueue_proto_controller_proto_rawDescOnce.Do(func() {
		file_kueue_proto_controller_proto_rawDescData = protoimpl.X.CompressGZIP(file_kueue_proto_controller_proto_rawDescData)
	})
	return file_kueue_proto_controller_proto_rawDescData
}

var file_kueue_proto_controller_proto_msgTypes = make([]protoimpl.MessageInfo, 10)
var file_kueue_proto_controller_proto_goTypes = []any{
	(*ProducerTopicRequest)(nil),   // 0: ProducerTopicRequest
	(*ProducerTopicResponse)(nil),  // 1: ProducerTopicResponse
	(*PartitionInfo)(nil),          // 2: PartitionInfo
	(*BrokerInfoProto)(nil),        // 3: BrokerInfoProto
	(*SubscribeRequest)(nil),       // 4: SubscribeRequest
	(*SubscribeResponse)(nil),      // 5: SubscribeResponse
	(*HeartbeatRequest)(nil),       // 6: HeartbeatRequest
	(*HeartbeatResponse)(nil),      // 7: HeartbeatResponse
	(*RegisterBrokerRequest)(nil),  // 8: RegisterBrokerRequest
	(*RegisterBrokerResponse)(nil), // 9: RegisterBrokerResponse
}
var file_kueue_proto_controller_proto_depIdxs = []int32{
	2, // 0: ProducerTopicResponse.partitions:type_name -> PartitionInfo
	3, // 1: PartitionInfo.leader:type_name -> BrokerInfoProto
	3, // 2: PartitionInfo.replicas:type_name -> BrokerInfoProto
	2, // 3: SubscribeResponse.partition:type_name -> PartitionInfo
	0, // 4: ControllerService.GetTopic:input_type -> ProducerTopicRequest
	8, // 5: ControllerService.RegisterBroker:input_type -> RegisterBrokerRequest
	4, // 6: ControllerService.Subscribe:input_type -> SubscribeRequest
	6, // 7: ControllerService.Heartbeat:input_type -> HeartbeatRequest
	1, // 8: ControllerService.GetTopic:output_type -> ProducerTopicResponse
	9, // 9: ControllerService.RegisterBroker:output_type -> RegisterBrokerResponse
	5, // 10: ControllerService.Subscribe:output_type -> SubscribeResponse
	7, // 11: ControllerService.Heartbeat:output_type -> HeartbeatResponse
	8, // [8:12] is the sub-list for method output_type
	4, // [4:8] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_kueue_proto_controller_proto_init() }
func file_kueue_proto_controller_proto_init() {
	if File_kueue_proto_controller_proto != nil {
		return
	}
	file_kueue_proto_controller_proto_msgTypes[0].OneofWrappers = []any{}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_kueue_proto_controller_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   10,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_kueue_proto_controller_proto_goTypes,
		DependencyIndexes: file_kueue_proto_controller_proto_depIdxs,
		MessageInfos:      file_kueue_proto_controller_proto_msgTypes,
	}.Build()
	File_kueue_proto_controller_proto = out.File
	file_kueue_proto_controller_proto_rawDesc = nil
	file_kueue_proto_controller_proto_goTypes = nil
	file_kueue_proto_controller_proto_depIdxs = nil
}
