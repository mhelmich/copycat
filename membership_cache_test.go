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
	config1.hostname = "127.0.0.1"
	config1.CopyCatDataDir = "./test-TestMembershipCacheBasic-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	m1, err := newMembershipCache(config1)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.hostname = "127.0.0.1"
	config2.CopyCatDataDir = "./test-TestMembershipCacheBasic-" + uint64ToString(randomRaftId()) + "/"
	config2.GossipPort = config1.GossipPort + 100
	config2.PeersToContact = make([]string, 1)
	config2.PeersToContact[0] = config1.hostname + ":" + strconv.Itoa(config1.GossipPort)
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
	dataStructureId := uint64(55)
	raftId := uint64(99)
	mockMembership := new(mockMemberList)
	mockMembership.On("addDataStructureToRaftIdMapping", dataStructureId, raftId).Return(nil)
	mockMembership.On("removeDataStructureToRaftIdMapping", raftId).Return(nil)

	msg := raftpb.Message{
		To: raftId,
	}

	mockRaftNode := new(mockRaftNode)
	mockRaftNode.On("Step", mock.Anything, msg).Return(nil)
	mockRaftNode.On("Stop").Return()

	mockBackend := &raftBackend{
		raftId:   raftId,
		raftNode: mockRaftNode,
		stopChan: make(chan struct{}, 1),
	}

	mc := &membershipCache{
		raftIdToRaftBackend: &sync.Map{},
		membership:          mockMembership,
		newDetachedRaftBackendWithIdFunc: func(newRaftId uint64, config *Config) (*raftBackend, error) {
			return mockBackend, nil
		},
	}

	backend, err := mc.newDetachedRaftBackend(dataStructureId, raftId, DefaultConfig())
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
	mockRaftNode.AssertNumberOfCalls(t, "Stop", 1)
}

func TestMembershipCacheCreateInteractiveRaftStepStop(t *testing.T) {
	dataStructureId := uint64(55)
	raftId := uint64(99)
	mockMembership := new(mockMemberList)
	mockMembership.On("addDataStructureToRaftIdMapping", dataStructureId, raftId).Return(nil)
	mockMembership.On("stop").Return(nil)

	msg := raftpb.Message{
		To: raftId,
	}

	mockRaftNode := new(mockRaftNode)
	mockRaftNode.On("Step", mock.Anything, msg).Return(nil)
	mockRaftNode.On("Stop").Return()

	mockBackend := &raftBackend{
		raftId:   raftId,
		raftNode: mockRaftNode,
		stopChan: make(chan struct{}, 1),
	}

	mc := &membershipCache{
		addressToConnection: &sync.Map{},
		raftIdToRaftBackend: &sync.Map{},
		membership:          mockMembership,
		newInteractiveRaftBackendFunc: func(config *Config, peers []pb.Peer, provider SnapshotProvider) (*raftBackend, error) {
			return mockBackend, nil
		},
	}

	backend, err := mc.newInteractiveRaftBackend(dataStructureId, DefaultConfig(), nil, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	assert.Equal(t, mockBackend, backend)
	mockMembership.AssertNumberOfCalls(t, "addDataStructureToRaftIdMapping", 1)

	err = mc.stepRaft(context.TODO(), msg)
	assert.Nil(t, err)
	mockRaftNode.AssertNumberOfCalls(t, "Step", 1)

	// test stop via stopping the entire cache
	err = mc.stop()
	assert.Nil(t, err)
}

func TestMembershipCacheStartRaftGroup(t *testing.T) {
	pickedPeers := make([]uint64, 2)
	pickedPeers[0] = uint64(12)
	pickedPeers[1] = uint64(24)

	networkAddress0 := "555_Fake_Street"
	networkAddress1 := "666_Fake_Street"

	mockMembership := new(mockMemberList)
	mockMembership.On("pickFromMetadata", mock.Anything, 2).Return(pickedPeers)
	mockMembership.On("getAddressForPeer", pickedPeers[0]).Return(networkAddress1)
	mockMembership.On("getAddressForPeer", pickedPeers[1]).Return(networkAddress0)

	resp1 := &pb.StartRaftResponse{
		RaftId:      uint64(99),
		RaftAddress: networkAddress1,
	}
	resp2 := &pb.StartRaftResponse{
		RaftId:      uint64(88),
		RaftAddress: networkAddress0,
	}
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
		chooserFunc: func(peerId uint64, tags map[string]string) bool {
			return true
		},
		logger: log.WithFields(log.Fields{}),
	}

	peers, err := mc.chooseReplicaNode(uint64(33), 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(peers))
	assert.Equal(t, uint64(88), peers[0].Id)
	assert.Equal(t, networkAddress0, peers[0].RaftAddress)
	assert.Equal(t, uint64(99), peers[1].Id)
	assert.Equal(t, networkAddress1, peers[1].RaftAddress)
	mockMembership.AssertNumberOfCalls(t, "pickFromMetadata", 1)
	mockMembership.AssertNumberOfCalls(t, "getAddressForPeer", 2)
	mockClient1.AssertNumberOfCalls(t, "StartRaft", 1)
	mockClient2.AssertNumberOfCalls(t, "StartRaft", 1)

	err = mc.stopRaftRemotely(peers[0])
	assert.Nil(t, err)
	err = mc.stopRaftRemotely(peers[1])
	assert.Nil(t, err)
	mockClient1.AssertNumberOfCalls(t, "StopRaft", 1)
	mockClient2.AssertNumberOfCalls(t, "StopRaft", 1)
}

func TestMembershipCacheCreateConnection(t *testing.T) {
	config := DefaultConfig()
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