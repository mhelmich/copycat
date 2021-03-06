// Code generated by mockery v1.0.0. DO NOT EDIT.
package copycat

import mock "github.com/stretchr/testify/mock"
import pb "github.com/mhelmich/copycat/pb"

// mockMemberList is an autogenerated mock type for the memberList type
type mockMemberList struct {
	mock.Mock
}

// addDataStructureToRaftIdMapping provides a mock function with given fields: dataStructureId, raftId
func (_m *mockMemberList) addDataStructureToRaftIdMapping(dataStructureId *ID, raftId uint64) error {
	ret := _m.Called(dataStructureId, raftId)

	var r0 error
	if rf, ok := ret.Get(0).(func(*ID, uint64) error); ok {
		r0 = rf(dataStructureId, raftId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// getAddressForPeer provides a mock function with given fields: peerId
func (_m *mockMemberList) getAddressForPeer(peerId uint64) string {
	ret := _m.Called(peerId)

	var r0 string
	if rf, ok := ret.Get(0).(func(uint64) string); ok {
		r0 = rf(peerId)
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// getAddressForRaftId provides a mock function with given fields: raftId
func (_m *mockMemberList) getAddressForRaftId(raftId uint64) string {
	ret := _m.Called(raftId)

	var r0 string
	if rf, ok := ret.Get(0).(func(uint64) string); ok {
		r0 = rf(raftId)
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// getNumReplicasForDataStructure provides a mock function with given fields: id
func (_m *mockMemberList) getNumReplicasForDataStructure(id *ID) int {
	ret := _m.Called(id)

	var r0 int
	if rf, ok := ret.Get(0).(func(*ID) int); ok {
		r0 = rf(id)
	} else {
		r0 = ret.Get(0).(int)
	}

	return r0
}

// onePeerForDataStructureId provides a mock function with given fields: dataStructureId
func (_m *mockMemberList) onePeerForDataStructureId(dataStructureId *ID) (*pb.RaftPeer, error) {
	ret := _m.Called(dataStructureId)

	var r0 *pb.RaftPeer
	if rf, ok := ret.Get(0).(func(*ID) *pb.RaftPeer); ok {
		r0 = rf(dataStructureId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*pb.RaftPeer)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(*ID) error); ok {
		r1 = rf(dataStructureId)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// peersForDataStructureId provides a mock function with given fields: dataStructureId
func (_m *mockMemberList) peersForDataStructureId(dataStructureId *ID) []*pb.RaftPeer {
	ret := _m.Called(dataStructureId)

	var r0 []*pb.RaftPeer
	if rf, ok := ret.Get(0).(func(*ID) []*pb.RaftPeer); ok {
		r0 = rf(dataStructureId)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*pb.RaftPeer)
		}
	}

	return r0
}

// pickReplicaPeers provides a mock function with given fields: dataStructureId, numReplicas
func (_m *mockMemberList) pickReplicaPeers(dataStructureId *ID, numReplicas int) []uint64 {
	ret := _m.Called(dataStructureId, numReplicas)

	var r0 []uint64
	if rf, ok := ret.Get(0).(func(*ID, int) []uint64); ok {
		r0 = rf(dataStructureId, numReplicas)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]uint64)
		}
	}

	return r0
}

// removeDataStructureToRaftIdMapping provides a mock function with given fields: raftId
func (_m *mockMemberList) removeDataStructureToRaftIdMapping(raftId uint64) error {
	ret := _m.Called(raftId)

	var r0 error
	if rf, ok := ret.Get(0).(func(uint64) error); ok {
		r0 = rf(raftId)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// stop provides a mock function with given fields:
func (_m *mockMemberList) stop() error {
	ret := _m.Called()

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}
