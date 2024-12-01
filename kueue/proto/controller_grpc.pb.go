// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.27.3
// source: kueue/proto/controller.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	ControllerService_GetTopicProducer_FullMethodName = "/ControllerService/GetTopicProducer"
	ControllerService_GetTopicConsumer_FullMethodName = "/ControllerService/GetTopicConsumer"
	ControllerService_RegisterBroker_FullMethodName   = "/ControllerService/RegisterBroker"
	ControllerService_Heartbeat_FullMethodName        = "/ControllerService/Heartbeat"
)

// ControllerServiceClient is the client API for ControllerService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type ControllerServiceClient interface {
	GetTopicProducer(ctx context.Context, in *ProducerTopicInfoRequest, opts ...grpc.CallOption) (*ProducerTopicInfoResponse, error)
	GetTopicConsumer(ctx context.Context, in *ConsumerTopicInfoRequest, opts ...grpc.CallOption) (*ConsumerTopicInfoResponse, error)
	RegisterBroker(ctx context.Context, in *RegisterBrokerRequest, opts ...grpc.CallOption) (*RegisterBrokerResponse, error)
	Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error)
}

type controllerServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewControllerServiceClient(cc grpc.ClientConnInterface) ControllerServiceClient {
	return &controllerServiceClient{cc}
}

func (c *controllerServiceClient) GetTopicProducer(ctx context.Context, in *ProducerTopicInfoRequest, opts ...grpc.CallOption) (*ProducerTopicInfoResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ProducerTopicInfoResponse)
	err := c.cc.Invoke(ctx, ControllerService_GetTopicProducer_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controllerServiceClient) GetTopicConsumer(ctx context.Context, in *ConsumerTopicInfoRequest, opts ...grpc.CallOption) (*ConsumerTopicInfoResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(ConsumerTopicInfoResponse)
	err := c.cc.Invoke(ctx, ControllerService_GetTopicConsumer_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controllerServiceClient) RegisterBroker(ctx context.Context, in *RegisterBrokerRequest, opts ...grpc.CallOption) (*RegisterBrokerResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(RegisterBrokerResponse)
	err := c.cc.Invoke(ctx, ControllerService_RegisterBroker_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *controllerServiceClient) Heartbeat(ctx context.Context, in *HeartbeatRequest, opts ...grpc.CallOption) (*HeartbeatResponse, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(HeartbeatResponse)
	err := c.cc.Invoke(ctx, ControllerService_Heartbeat_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ControllerServiceServer is the server API for ControllerService service.
// All implementations must embed UnimplementedControllerServiceServer
// for forward compatibility.
type ControllerServiceServer interface {
	GetTopicProducer(context.Context, *ProducerTopicInfoRequest) (*ProducerTopicInfoResponse, error)
	GetTopicConsumer(context.Context, *ConsumerTopicInfoRequest) (*ConsumerTopicInfoResponse, error)
	RegisterBroker(context.Context, *RegisterBrokerRequest) (*RegisterBrokerResponse, error)
	Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error)
	mustEmbedUnimplementedControllerServiceServer()
}

// UnimplementedControllerServiceServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedControllerServiceServer struct{}

func (UnimplementedControllerServiceServer) GetTopicProducer(context.Context, *ProducerTopicInfoRequest) (*ProducerTopicInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetTopicProducer not implemented")
}
func (UnimplementedControllerServiceServer) GetTopicConsumer(context.Context, *ConsumerTopicInfoRequest) (*ConsumerTopicInfoResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetTopicConsumer not implemented")
}
func (UnimplementedControllerServiceServer) RegisterBroker(context.Context, *RegisterBrokerRequest) (*RegisterBrokerResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method RegisterBroker not implemented")
}
func (UnimplementedControllerServiceServer) Heartbeat(context.Context, *HeartbeatRequest) (*HeartbeatResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method Heartbeat not implemented")
}
func (UnimplementedControllerServiceServer) mustEmbedUnimplementedControllerServiceServer() {}
func (UnimplementedControllerServiceServer) testEmbeddedByValue()                           {}

// UnsafeControllerServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to ControllerServiceServer will
// result in compilation errors.
type UnsafeControllerServiceServer interface {
	mustEmbedUnimplementedControllerServiceServer()
}

func RegisterControllerServiceServer(s grpc.ServiceRegistrar, srv ControllerServiceServer) {
	// If the following call pancis, it indicates UnimplementedControllerServiceServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&ControllerService_ServiceDesc, srv)
}

func _ControllerService_GetTopicProducer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ProducerTopicInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControllerServiceServer).GetTopicProducer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControllerService_GetTopicProducer_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControllerServiceServer).GetTopicProducer(ctx, req.(*ProducerTopicInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControllerService_GetTopicConsumer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(ConsumerTopicInfoRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControllerServiceServer).GetTopicConsumer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControllerService_GetTopicConsumer_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControllerServiceServer).GetTopicConsumer(ctx, req.(*ConsumerTopicInfoRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControllerService_RegisterBroker_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(RegisterBrokerRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControllerServiceServer).RegisterBroker(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControllerService_RegisterBroker_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControllerServiceServer).RegisterBroker(ctx, req.(*RegisterBrokerRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _ControllerService_Heartbeat_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(HeartbeatRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(ControllerServiceServer).Heartbeat(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: ControllerService_Heartbeat_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(ControllerServiceServer).Heartbeat(ctx, req.(*HeartbeatRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// ControllerService_ServiceDesc is the grpc.ServiceDesc for ControllerService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var ControllerService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "ControllerService",
	HandlerType: (*ControllerServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetTopicProducer",
			Handler:    _ControllerService_GetTopicProducer_Handler,
		},
		{
			MethodName: "GetTopicConsumer",
			Handler:    _ControllerService_GetTopicConsumer_Handler,
		},
		{
			MethodName: "RegisterBroker",
			Handler:    _ControllerService_RegisterBroker_Handler,
		},
		{
			MethodName: "Heartbeat",
			Handler:    _ControllerService_Heartbeat_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "kueue/proto/controller.proto",
}
