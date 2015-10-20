// Code generated by protoc-gen-go.
// source: pps/watch/watch.proto
// DO NOT EDIT!

/*
Package watch is a generated protocol buffer package.

It is generated from these files:
	pps/watch/watch.proto

It has these top-level messages:
	ChangeEvent
*/
package watch

import proto "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"
import google_protobuf "go.pedge.io/google-protobuf"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type ChangeEvent_Type int32

const (
	ChangeEvent_CHANGE_EVENT_TYPE_NONE   ChangeEvent_Type = 0
	ChangeEvent_CHANGE_EVENT_TYPE_CREATE ChangeEvent_Type = 1
	ChangeEvent_CHANGE_EVENT_TYPE_DELETE ChangeEvent_Type = 2
)

var ChangeEvent_Type_name = map[int32]string{
	0: "CHANGE_EVENT_TYPE_NONE",
	1: "CHANGE_EVENT_TYPE_CREATE",
	2: "CHANGE_EVENT_TYPE_DELETE",
}
var ChangeEvent_Type_value = map[string]int32{
	"CHANGE_EVENT_TYPE_NONE":   0,
	"CHANGE_EVENT_TYPE_CREATE": 1,
	"CHANGE_EVENT_TYPE_DELETE": 2,
}

func (x ChangeEvent_Type) String() string {
	return proto.EnumName(ChangeEvent_Type_name, int32(x))
}

type ChangeEvent struct {
	Type         ChangeEvent_Type `protobuf:"varint,1,opt,name=type,enum=pachyderm.pps.watch.ChangeEvent_Type" json:"type,omitempty"`
	PipelineName string           `protobuf:"bytes,2,opt,name=pipeline_name" json:"pipeline_name,omitempty"`
}

func (m *ChangeEvent) Reset()         { *m = ChangeEvent{} }
func (m *ChangeEvent) String() string { return proto.CompactTextString(m) }
func (*ChangeEvent) ProtoMessage()    {}

func init() {
	proto.RegisterEnum("pachyderm.pps.watch.ChangeEvent_Type", ChangeEvent_Type_name, ChangeEvent_Type_value)
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// Client API for API service

type APIClient interface {
	Start(ctx context.Context, in *google_protobuf.Empty, opts ...grpc.CallOption) (*google_protobuf.Empty, error)
	RegisterChangeEvent(ctx context.Context, in *ChangeEvent, opts ...grpc.CallOption) (*google_protobuf.Empty, error)
}

type aPIClient struct {
	cc *grpc.ClientConn
}

func NewAPIClient(cc *grpc.ClientConn) APIClient {
	return &aPIClient{cc}
}

func (c *aPIClient) Start(ctx context.Context, in *google_protobuf.Empty, opts ...grpc.CallOption) (*google_protobuf.Empty, error) {
	out := new(google_protobuf.Empty)
	err := grpc.Invoke(ctx, "/pachyderm.pps.watch.API/Start", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *aPIClient) RegisterChangeEvent(ctx context.Context, in *ChangeEvent, opts ...grpc.CallOption) (*google_protobuf.Empty, error) {
	out := new(google_protobuf.Empty)
	err := grpc.Invoke(ctx, "/pachyderm.pps.watch.API/RegisterChangeEvent", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for API service

type APIServer interface {
	Start(context.Context, *google_protobuf.Empty) (*google_protobuf.Empty, error)
	RegisterChangeEvent(context.Context, *ChangeEvent) (*google_protobuf.Empty, error)
}

func RegisterAPIServer(s *grpc.Server, srv APIServer) {
	s.RegisterService(&_API_serviceDesc, srv)
}

func _API_Start_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(google_protobuf.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(APIServer).Start(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _API_RegisterChangeEvent_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(ChangeEvent)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(APIServer).RegisterChangeEvent(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

var _API_serviceDesc = grpc.ServiceDesc{
	ServiceName: "pachyderm.pps.watch.API",
	HandlerType: (*APIServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Start",
			Handler:    _API_Start_Handler,
		},
		{
			MethodName: "RegisterChangeEvent",
			Handler:    _API_RegisterChangeEvent_Handler,
		},
	},
	Streams: []grpc.StreamDesc{},
}
