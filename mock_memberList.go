// Code generated by mockery v1.0.0. DO NOT EDIT.
package copycat

import mock "github.com/stretchr/testify/mock"
import pb "github.com/mhelmich/copycat/pb"

// mockMemberList is an autogenerated mock type for the memberList type
type mockMemberList struct {
	mock.Mock
}

// addDataStructureToRaftIdMapping provides a mock function with given fields: dataStructureId, raftId
func (_m *mockMemberList) addDataStructureToRaftIdMapping(dataStructureId uint64, raftId uint64) error {
	ret := _m.Called(dataStructureId, raftId)

	var r0 error
	if rf, ok := ret.Get(0).(func(uint64, uint64) error); ok {
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

// myGossipNodeId provides a mock function with given fields:
func (_m *mockMemberList) myGossipNodeId() uint64 {
	ret := _m.Called()

	var r0 uint64
	if rf, ok := ret.Get(0).(func() uint64); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(uint64)
	}

	return r0
}

// onePeerForDataStructureId provides a mock function with given fields: dataStructureId
func (_m *mockMemberList) onePeerForDataStructureId(dataStructureId uint64) (*pb.Peer, error) {
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
func (_m *mockMemberList) peersForDataStructureId(dataStructureId uint64) []pb.Peer {
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

// pickFromMetadata provides a mock function with given fields: picker, numItemsToPick, avoidMe
func (_m *mockMemberList) pickFromMetadata(picker func(uint64, map[string]string) bool, numItemsToPick int, avoidMe []uint64) []uint64 {
	ret := _m.Called(picker, numItemsToPick, avoidMe)

	var r0 []uint64
	if rf, ok := ret.Get(0).(func(func(uint64, map[string]string) bool, int, []uint64) []uint64); ok {
		r0 = rf(picker, numItemsToPick, avoidMe)
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
