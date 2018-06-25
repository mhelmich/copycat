/*
 * Copyright 2018 Marco Helmich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package copycat

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

func TestMembershipCacheBasic(t *testing.T) {
	config1 := DefaultConfig()
	config1.Hostname = "127.0.0.1"
	config1.logger = log.WithFields(log.Fields{})
	config1.CopyCatDataDir = "./test-TestMembershipCacheBasic-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	m1, err := newMembershipCache(config1)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.Hostname = "127.0.0.1"
	config2.logger = log.WithFields(log.Fields{})
	config2.CopyCatDataDir = "./test-TestMembershipCacheBasic-" + uint64ToString(randomRaftId()) + "/"
	config2.GossipPort = config1.GossipPort + 100
	config2.PeersToContact = make([]string, 1)
	config2.PeersToContact[0] = config1.Hostname + ":" + strconv.Itoa(config1.GossipPort)
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	m2, err := newMembershipCache(config2)
	assert.Nil(t, err)

	err = m1.stop()
	assert.Nil(t, err)
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = m2.stop()
	assert.Nil(t, err)
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestMembershipCacheCreateDetachedRaftStepStop(t *testing.T) {
	dataStructureId, err := newId()
	assert.Nil(t, err)
	raftId := uint64(99)
	mockMembership := new(mockMemberList)
	mockMembership.On("addDataStructureToRaftIdMapping", dataStructureId, raftId).Return(nil)
	mockMembership.On("removeDataStructureToRaftIdMapping", raftId).Return(nil)

	msg := raftpb.Message{
		To: raftId,
	}

	mockRaftNode := new(mockRaftNode)
	mockRaftNode.On("Step", mock.Anything, msg).Return(nil)
	mockRaftNode.On("ProposeConfChange", mock.Anything, mock.Anything).Return(nil)
	mockRaftNode.On("Stop").Return()

	mockBackend := &raftBackend{
		raftId:   raftId,
		raftNode: mockRaftNode,
		logger:   log.WithFields(log.Fields{}),
		stopChan: make(chan struct{}, 1),
	}

	mc := &membershipCache{
		raftIdToRaftBackend: &sync.Map{},
		membership:          mockMembership,
		newDetachedRaftBackendWithIdAndPeersFunc: func(newRaftId uint64, config *Config, peers []*pb.RaftPeer) (*raftBackend, error) {
			return mockBackend, nil
		},
		logger: log.WithFields(log.Fields{}),
	}

	backend, err := mc.newDetachedRaftBackend(dataStructureId, raftId, DefaultConfig(), nil)
	assert.Nil(t, err)
	assert.Equal(t, mockBackend, backend)
	mockMembership.AssertNumberOfCalls(t, "addDataStructureToRaftIdMapping", 1)

	err = mc.stepRaft(context.TODO(), raftpb.Message{
		To: uint64(123),
	})
	assert.NotNil(t, err)
	mockRaftNode.AssertNumberOfCalls(t, "Step", 0)

	err = mc.stepRaft(context.TODO(), msg)
	assert.Nil(t, err)
	mockRaftNode.AssertNumberOfCalls(t, "Step", 1)

	mc.stopRaft(raftId)
	mockRaftNode.AssertNumberOfCalls(t, "ProposeConfChange", 1)
}

func TestMembershipCacheCreateInteractiveRaftStepStop(t *testing.T) {
	dataStructureId, err := newId()
	assert.Nil(t, err)
	raftId := uint64(99)
	mockMembership := new(mockMemberList)
	mockMembership.On("addDataStructureToRaftIdMapping", dataStructureId, raftId).Return(nil)
	mockMembership.On("stop").Return(nil)

	msg := raftpb.Message{
		To: raftId,
	}

	mockRaftNode := new(mockRaftNode)
	mockRaftNode.On("Step", mock.Anything, msg).Return(nil)
	mockRaftNode.On("ProposeConfChange", mock.Anything, mock.Anything).Return(nil)
	mockRaftNode.On("Stop").Return()

	mockBackend := &raftBackend{
		raftId:   raftId,
		raftNode: mockRaftNode,
		stopChan: make(chan struct{}, 1),
		logger:   log.WithFields(log.Fields{}),
	}

	mc := &membershipCache{
		addressToConnection: &sync.Map{},
		raftIdToRaftBackend: &sync.Map{},
		membership:          mockMembership,
		newInteractiveRaftBackendForExistingGroupFunc: func(config *Config, provider SnapshotProvider) (*raftBackend, error) {
			return mockBackend, nil
		},
		logger: log.WithFields(log.Fields{}),
	}

	backend, err := mc.newInteractiveRaftBackendForExistingGroup(dataStructureId, DefaultConfig(), func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	assert.Equal(t, mockBackend, backend)
	mockMembership.AssertNumberOfCalls(t, "addDataStructureToRaftIdMapping", 1)

	err = mc.stepRaft(context.TODO(), msg)
	assert.Nil(t, err)
	mockRaftNode.AssertNumberOfCalls(t, "Step", 1)

	// test stop via stopping the entire cache
	err = mc.stop()
	assert.Nil(t, err)
	mockRaftNode.AssertNumberOfCalls(t, "ProposeConfChange", 1)
}

func TestMembershipCacheStartRaftGroup(t *testing.T) {
	pickedPeers := make([]uint64, 2)
	pickedPeers[0] = uint64(12)
	pickedPeers[1] = uint64(24)

	networkAddress0 := "555_Fake_Street"
	networkAddress1 := "666_Fake_Street"

	mockMembership := new(mockMemberList)
	mockMembership.On("pickReplicaPeers", mock.Anything, 2, mock.Anything).Return(pickedPeers)
	mockMembership.On("getAddressForPeer", pickedPeers[0]).Return(networkAddress1)
	mockMembership.On("getAddressForPeer", pickedPeers[1]).Return(networkAddress0)

	resp1 := &pb.StartRaftResponse{}
	resp2 := &pb.StartRaftResponse{}
	mockClient1 := new(mockCopyCatServiceClient)
	mockClient1.On("StartRaft", mock.Anything, mock.Anything).Return(resp1, nil)
	mockClient1.On("StopRaft", mock.Anything, mock.Anything).Return(nil, nil)

	mockClient2 := new(mockCopyCatServiceClient)
	mockClient2.On("StartRaft", mock.Anything, mock.Anything).Return(resp2, nil)
	mockClient2.On("StopRaft", mock.Anything, mock.Anything).Return(nil, nil)

	mc := &membershipCache{
		membership: mockMembership,
		newCopyCatServiceClientFunc: func(conn *grpc.ClientConn) pb.CopyCatServiceClient {
			// HACK
			// see connectionCacheFunc
			if conn == nil {
				return mockClient2
			}
			return mockClient1
		},
		connectionCacheFunc: func(m *sync.Map, address string) (*grpc.ClientConn, error) {
			// HACK
			// this is a little bit of a hack
			// I encode two different state into the conection being nil or not
			// with that I make newCopyCatServiceClientFunc hand out two different clients
			if address == networkAddress1 {
				// HACK
				// there's a race between these two go routines
				// I need to make one wait for the other
				time.Sleep(50 * time.Millisecond)
				return &grpc.ClientConn{}, nil
			} else if address == networkAddress0 {
				return nil, nil
			}
			return nil, fmt.Errorf("Unknown network address ... you didn't set the test up correctly!")
		},
		logger: log.WithFields(log.Fields{}),
	}

	dataStructureId, err := newId()
	assert.Nil(t, err)
	peers, err := mc.startNewRaftGroup(dataStructureId, 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(peers))
	assert.Equal(t, networkAddress0, peers[1].PeerAddress)
	assert.Equal(t, networkAddress1, peers[0].PeerAddress)
	mockMembership.AssertNumberOfCalls(t, "pickReplicaPeers", 1)
	mockMembership.AssertNumberOfCalls(t, "getAddressForPeer", 2)
	mockClient1.AssertNumberOfCalls(t, "StartRaft", 1)
	mockClient2.AssertNumberOfCalls(t, "StartRaft", 1)

	err = mc.stopRaftRemotely(*peers[0])
	assert.Nil(t, err)
	err = mc.stopRaftRemotely(*peers[1])
	assert.Nil(t, err)
	mockClient1.AssertNumberOfCalls(t, "StopRaft", 1)
	mockClient2.AssertNumberOfCalls(t, "StopRaft", 1)
}

func TestMembershipCacheStartRaftGroupWithFailure(t *testing.T) {
	pickedPeers := make([]uint64, 2)
	pickedPeers[0] = uint64(12)
	pickedPeers[1] = uint64(24)

	networkAddress := "555_Fake_Street"

	mockMembership := new(mockMemberList)
	mockMembership.On("pickReplicaPeers", mock.Anything, 1, mock.Anything).Return(pickedPeers)
	mockMembership.On("getAddressForPeer", pickedPeers[0]).Return("")
	mockMembership.On("getAddressForPeer", pickedPeers[1]).Return(networkAddress)

	resp := &pb.StartRaftResponse{}
	mockClient := new(mockCopyCatServiceClient)
	mockClient.On("StartRaft", mock.Anything, mock.Anything).Return(resp, nil)
	mockClient.On("StopRaft", mock.Anything, mock.Anything).Return(nil, nil)

	mc := &membershipCache{
		membership: mockMembership,
		newCopyCatServiceClientFunc: func(conn *grpc.ClientConn) pb.CopyCatServiceClient {
			return mockClient
		},
		connectionCacheFunc: func(m *sync.Map, address string) (*grpc.ClientConn, error) {
			return &grpc.ClientConn{}, nil
		},
		logger: log.WithFields(log.Fields{}),
	}

	dataStructureId, err := newId()
	assert.Nil(t, err)
	peers, err := mc.startNewRaftGroup(dataStructureId, 1)
	assert.NotNil(t, err)
	assert.Nil(t, peers)
	mockMembership.AssertNumberOfCalls(t, "pickReplicaPeers", 4)
	mockMembership.AssertNumberOfCalls(t, "getAddressForPeer", 16)
	mockClient.AssertNumberOfCalls(t, "StartRaft", 4)
	mockClient.AssertNumberOfCalls(t, "StopRaft", 4)
}

func TestMembershipCacheCreateConnection(t *testing.T) {
	config := DefaultConfig()
	config.logger = log.WithFields(log.Fields{})
	m := new(mockMembershipProxy)
	transport, err := newTransport(config, m)
	assert.Nil(t, err)

	address := config.address()
	mp := &sync.Map{}
	conn1, err := _getConnectionForAddress(mp, address)
	assert.Nil(t, err)

	sizeOfMap := 0
	mp.Range(func(key, value interface{}) bool {
		sizeOfMap++
		return true
	})

	assert.Equal(t, 1, sizeOfMap)
	v, ok := mp.Load(address)
	assert.True(t, ok)
	assert.NotNil(t, v)

	conn2, err := _getConnectionForAddress(mp, address)
	assert.Nil(t, err)
	assert.Equal(t, conn1, conn2)

	err = conn2.Close()
	assert.Nil(t, err)
	err = conn1.Close()
	assert.NotNil(t, err)
	transport.stop()
}

func TestMembershipCacheAddToRaftGroup(t *testing.T) {
	newRaftId := uint64(99)
	mockRaftNode := new(mockRaftNode)
	mockRaftNode.On("ProposeConfChange", mock.Anything, mock.MatchedBy(func(cc raftpb.ConfChange) bool { return cc.NodeID == newRaftId })).Return(nil)

	mockBackend := &raftBackend{
		raftId:   randomRaftId(),
		raftNode: mockRaftNode,
		stopChan: make(chan struct{}, 1),
	}

	mc := &membershipCache{
		raftIdToRaftBackend: &sync.Map{},
	}

	mc.raftIdToRaftBackend.Store(mockBackend.raftId, mockBackend)
	// negative test
	err := mc.addToRaftGroup(context.TODO(), uint64(85), newRaftId)
	assert.NotNil(t, err)
	mockRaftNode.AssertNumberOfCalls(t, "ProposeConfChange", 0)
	// positive test
	err = mc.addToRaftGroup(context.TODO(), mockBackend.raftId, newRaftId)
	assert.Nil(t, err)
	mockRaftNode.AssertNumberOfCalls(t, "ProposeConfChange", 1)
}
