// Code generated by mockery v1.0.0. DO NOT EDIT.
package copycat

import (
	context "context"

	grpc "google.golang.org/grpc"

	"github.com/mhelmich/copycat/pb"
	mock "github.com/stretchr/testify/mock"
)

// mockRaftTransportServiceClient is an autogenerated mock type for the RaftTransportServiceClient type
type mockRaftTransportServiceClient struct {
	mock.Mock
}

// Send provides a mock function with given fields: ctx, opts
func (_m *mockRaftTransportServiceClient) Send(ctx context.Context, opts ...grpc.CallOption) (pb.RaftTransportService_SendClient, error) {
	_va := make([]interface{}, len(opts))
	for _i := range opts {
		_va[_i] = opts[_i]
	}
	var _ca []interface{}
	_ca = append(_ca, ctx)
	_ca = append(_ca, _va...)
	ret := _m.Called(_ca...)

	var r0 pb.RaftTransportService_SendClient
	if rf, ok := ret.Get(0).(func(context.Context, ...grpc.CallOption) pb.RaftTransportService_SendClient); ok {
		r0 = rf(ctx, opts...)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pb.RaftTransportService_SendClient)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, ...grpc.CallOption) error); ok {
		r1 = rf(ctx, opts...)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}