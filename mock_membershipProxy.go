// Code generated by mockery v1.0.0. DO NOT EDIT.
package copycat

import context "context"
import mock "github.com/stretchr/testify/mock"
import pb "github.com/mhelmich/copycat/pb"
import raftpb "github.com/coreos/etcd/raft/raftpb"

// mockMembershipProxy is an autogenerated mock type for the membershipProxy type
type mockMembershipProxy struct {
	mock.Mock
}

// addRaftToGroupRemotely provides a mock function with given fields: newRaftId, peers
func (_m *mockMembershipProxy) addRaftToGroupRemotely(newRaftId uint64, peers *pb.Peer) error {
	ret := _m.Called(newRaftId, peers)

	var r0 error
	if rf, ok := ret.Get(0).(func(uint64, *pb.Peer) error); ok {
		r0 = rf(newRaftId, peers)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// addToRaftGroup provides a mock function with given fields: ctx, existingRaftId, newRaftId
func (_m *mockMembershipProxy) addToRaftGroup(ctx context.Context, existingRaftId uint64, newRaftId uint64) error {
	ret := _m.Called(ctx, existingRaftId, newRaftId)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, uint64, uint64) error); ok {
		r0 = rf(ctx, existingRaftId, newRaftId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// chooseReplicaNode provides a mock function with given fields: dataStructureId, numReplicas
func (_m *mockMembershipProxy) chooseReplicaNode(dataStructureId uint64, numReplicas int) ([]pb.Peer, error) {
	ret := _m.Called(dataStructureId, numReplicas)

	var r0 []pb.Peer
	if rf, ok := ret.Get(0).(func(uint64, int) []pb.Peer); ok {
		r0 = rf(dataStructureId, numReplicas)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]pb.Peer)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint64, int) error); ok {
		r1 = rf(dataStructureId, numReplicas)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// getRaftTransportServiceClientForRaftId provides a mock function with given fields: raftId
func (_m *mockMembershipProxy) getRaftTransportServiceClientForRaftId(raftId uint64) (pb.RaftTransportServiceClient, error) {
	ret := _m.Called(raftId)

	var r0 pb.RaftTransportServiceClient
	if rf, ok := ret.Get(0).(func(uint64) pb.RaftTransportServiceClient); ok {
		r0 = rf(raftId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(pb.RaftTransportServiceClient)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint64) error); ok {
		r1 = rf(raftId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// newDetachedRaftBackend provides a mock function with given fields: dataStructureId, raftId, config
func (_m *mockMembershipProxy) newDetachedRaftBackend(dataStructureId uint64, raftId uint64, config *Config) (*raftBackend, error) {
	ret := _m.Called(dataStructureId, raftId, config)

	var r0 *raftBackend
	if rf, ok := ret.Get(0).(func(uint64, uint64, *Config) *raftBackend); ok {
		r0 = rf(dataStructureId, raftId, config)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*raftBackend)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint64, uint64, *Config) error); ok {
		r1 = rf(dataStructureId, raftId, config)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// newInteractiveRaftBackend provides a mock function with given fields: dataStructureId, config, peers, provider
func (_m *mockMembershipProxy) newInteractiveRaftBackend(dataStructureId uint64, config *Config, peers []pb.Peer, provider SnapshotProvider) (*raftBackend, error) {
	ret := _m.Called(dataStructureId, config, peers, provider)

	var r0 *raftBackend
	if rf, ok := ret.Get(0).(func(uint64, *Config, []pb.Peer, SnapshotProvider) *raftBackend); ok {
		r0 = rf(dataStructureId, config, peers, provider)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*raftBackend)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint64, *Config, []pb.Peer, SnapshotProvider) error); ok {
		r1 = rf(dataStructureId, config, peers, provider)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// newInteractiveRaftBackendForExistingGroup provides a mock function with given fields: dataStructureId, config, provider
func (_m *mockMembershipProxy) newInteractiveRaftBackendForExistingGroup(dataStructureId uint64, config *Config, provider SnapshotProvider) (*raftBackend, error) {
	ret := _m.Called(dataStructureId, config, provider)

	var r0 *raftBackend
	if rf, ok := ret.Get(0).(func(uint64, *Config, SnapshotProvider) *raftBackend); ok {
		r0 = rf(dataStructureId, config, provider)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*raftBackend)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint64, *Config, SnapshotProvider) error); ok {
		r1 = rf(dataStructureId, config, provider)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// onePeerForDataStructureId provides a mock function with given fields: dataStructureId
func (_m *mockMembershipProxy) onePeerForDataStructureId(dataStructureId uint64) (*pb.Peer, error) {
	ret := _m.Called(dataStructureId)

	var r0 *pb.Peer
	if rf, ok := ret.Get(0).(func(uint64) *pb.Peer); ok {
		r0 = rf(dataStructureId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*pb.Peer)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(uint64) error); ok {
		r1 = rf(dataStructureId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// peersForDataStructureId provides a mock function with given fields: dataStructureId
func (_m *mockMembershipProxy) peersForDataStructureId(dataStructureId uint64) []pb.Peer {
	ret := _m.Called(dataStructureId)

	var r0 []pb.Peer
	if rf, ok := ret.Get(0).(func(uint64) []pb.Peer); ok {
		r0 = rf(dataStructureId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]pb.Peer)
		}
	}

	return r0
}

// stepRaft provides a mock function with given fields: ctx, msg
func (_m *mockMembershipProxy) stepRaft(ctx context.Context, msg raftpb.Message) error {
	ret := _m.Called(ctx, msg)

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context, raftpb.Message) error); ok {
		r0 = rf(ctx, msg)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// stop provides a mock function with given fields:
func (_m *mockMembershipProxy) stop() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// stopRaft provides a mock function with given fields: raftId
func (_m *mockMembershipProxy) stopRaft(raftId uint64) error {
	ret := _m.Called(raftId)

	var r0 error
	if rf, ok := ret.Get(0).(func(uint64) error); ok {
		r0 = rf(raftId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// stopRaftRemotely provides a mock function with given fields: peer
func (_m *mockMembershipProxy) stopRaftRemotely(peer pb.Peer) error {
	ret := _m.Called(peer)

	var r0 error
	if rf, ok := ret.Get(0).(func(pb.Peer) error); ok {
		r0 = rf(peer)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
