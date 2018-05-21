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
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

func TestCopyCatBasic(t *testing.T) {
	config1 := DefaultConfig()
	config1.CopyCatDataDir = "./test-TestCopyCatBasic-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc1, err := newCopyCat(config1)
	assert.Nil(t, err)

	cc1.Shutdown()
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestCopyCatNewDataStructure(t *testing.T) {
	newDataStructureId := randomRaftId()
	config1 := DefaultConfig()
	config1.CopyCatDataDir = "./test-TestCopyCatNewDataStructure-" + uint64ToString(randomRaftId()) + "/"
	config1.GossipPort = config1.GossipPort + 22222
	config1.CopyCatPort = config1.CopyCatPort + 22222
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc1, err := newCopyCat(config1)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.CopyCatDataDir = "./test-TestCopyCatNewDataStructure-" + uint64ToString(randomRaftId()) + "/"
	config2.GossipPort = config1.GossipPort + 10000
	config2.CopyCatPort = config1.CopyCatPort + 10000
	config2.PeersToContact = make([]string, 1)
	config2.PeersToContact[0] = config1.hostname + ":" + strconv.Itoa(config1.GossipPort)
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc2, err := newCopyCat(config2)
	assert.Nil(t, err)

	peers, err := cc1.choosePeersForNewDataStructure(newDataStructureId, cc1.membership.getAllMetadata(), 1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(peers))
	assert.Equal(t, config2.hostname+":"+strconv.Itoa(config2.CopyCatPort), peers[0].RaftAddress)

	cc1.Shutdown()
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)

	cc2.Shutdown()
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestStartRaftGroup(t *testing.T) {
	host, _ := os.Hostname()
	localRaftIdToAssetOn := randomRaftId()
	remoteRaftIdToAssetOn := randomRaftId()
	addressToContact := "address_to_contact"
	startResp := &pb.StartRaftResponse{
		RaftId:      remoteRaftIdToAssetOn,
		RaftAddress: addressToContact,
	}
	mockClient := new(mockClient)
	mockClient.On("StartRaft", mock.Anything, mock.Anything, mock.Anything).Return(startResp, nil)

	mockMembership := new(mockCopyCatMembership)
	// it doesn't even matter what is in those maps
	// because I'm always returning the same client :)
	metadataMap := make(map[uint64]map[string]string)
	metadataMap[randomRaftId()] = make(map[string]string)
	metadataMap[randomRaftId()] = make(map[string]string)
	mockMembership.On("getAllMetadata").Return(metadataMap)
	mockMembership.On("getAddr", mock.Anything).Return(addressToContact)
	mockMembership.On("addDataStructureToRaftIdMapping", mock.Anything, mock.Anything).Return(nil)

	mockCopyCatClientFunc := func(*sync.Map, string) (pb.CopyCatServiceClient, error) {
		return mockClient, nil
	}

	peersCh := make(chan *[]pb.Peer, 1)
	mockInteractiveRaftBackendFunc := func(config *Config, peers []pb.Peer, provider SnapshotProvider) (*raftBackend, error) {
		peersCh <- &peers
		return &raftBackend{raftId: localRaftIdToAssetOn}, nil
	}

	cc := &copyCatImpl{
		myAddress:                     host + ":" + strconv.Itoa(5599),
		addressToConnection:           &sync.Map{},
		membership:                    mockMembership,
		newCopyCatClientFunc:          mockCopyCatClientFunc,
		newInteractiveRaftBackendFunc: mockInteractiveRaftBackendFunc,
		logger: log.WithFields(log.Fields{
			"test": "TestStartRaftGroup",
		}),
	}

	dsId := randomRaftId()
	rb, err := cc.startNewRaftGroup(dsId, 1, func() ([]byte, error) { return make([]byte, 0), nil })

	assert.Nil(t, err)
	assert.NotNil(t, rb)
	peersToAssertOn := *<-peersCh
	assert.Equal(t, 1, len(peersToAssertOn))
	assert.Equal(t, addressToContact, peersToAssertOn[0].RaftAddress)
	assert.Equal(t, remoteRaftIdToAssetOn, peersToAssertOn[0].Id)
	mockClient.AssertNumberOfCalls(t, "StartRaft", 1)
	mockClient.AssertNumberOfCalls(t, "StopRaft", 0)
	mockMembership.AssertNumberOfCalls(t, "getAllMetadata", 1)
	mockMembership.AssertNumberOfCalls(t, "getAddr", 1)
	mockMembership.AssertNumberOfCalls(t, "addDataStructureToRaftIdMapping", 1)
	assert.Equal(t, localRaftIdToAssetOn, rb.raftId)
}

func TestCopyCatConnectToDataStructureExisting(t *testing.T) {
	host, _ := os.Hostname()
	mockMembership := new(mockCopyCatMembership)
	mockPeers := make([]pb.Peer, 3)
	mockPeers[0] = pb.Peer{
		Id:          randomRaftId(),
		RaftAddress: "",
	}
	mockPeers[1] = pb.Peer{
		Id:          randomRaftId(),
		RaftAddress: "",
	}
	mockPeers[2] = pb.Peer{
		Id:          randomRaftId(),
		RaftAddress: "",
	}

	mockMembership.On("peersForDataStructureId", mock.Anything).Return(mockPeers)
	mockMembership.On("addDataStructureToRaftIdMapping", mock.Anything, mock.Anything).Return(nil)

	mockClient := new(mockClient)

	cc := &copyCatImpl{
		myAddress:  host + ":" + strconv.Itoa(5599),
		membership: mockMembership,
		newCopyCatClientFunc: func(*sync.Map, string) (pb.CopyCatServiceClient, error) {
			return mockClient, nil
		},
		newInteractiveRaftBackendFunc: func(config *Config, peers []pb.Peer, provider SnapshotProvider) (*raftBackend, error) {
			return &raftBackend{
				proposeChan: make(chan []byte),
				commitChan:  make(chan []byte),
				errorChan:   make(chan error),
			}, nil
		},
		logger: log.WithFields(log.Fields{
			"test": "TestCopyCatConnectToDataStructureExisting",
		}),
	}

	myDataStructureId := randomRaftId()
	proposeCh, commitCh, errorCh, snapshotConsumer := cc.ConnectToDataStructure(myDataStructureId, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.NotNil(t, proposeCh)
	assert.NotNil(t, commitCh)
	assert.NotNil(t, errorCh)
	assert.NotNil(t, snapshotConsumer)
	mockMembership.AssertNumberOfCalls(t, "peersForDataStructureId", 1)
	mockMembership.AssertNumberOfCalls(t, "addDataStructureToRaftIdMapping", 1)
}

func TestCopyCatConnectToDataStructureNew(t *testing.T) {
	mockMembership := new(mockCopyCatMembership)
	mockPeers := make([]pb.Peer, 0)
	mockMembership.On("peersForDataStructureId", mock.Anything).Return(mockPeers)
	mockMembership.On("addDataStructureToRaftIdMapping", mock.Anything, mock.Anything).Return(nil)
	metadataMap := make(map[uint64]map[string]string)
	metadataMap[randomRaftId()] = make(map[string]string)
	metadataMap[randomRaftId()] = make(map[string]string)
	metadataMap[randomRaftId()] = make(map[string]string)
	mockMembership.On("getAllMetadata").Return(metadataMap)
	mockMembership.On("getAddr", mock.Anything).Return("address_to_contact")

	mockClient := new(mockClient)
	aRandomRaftId := randomRaftId()
	startResp := &pb.StartRaftResponse{
		RaftId:      aRandomRaftId,
		RaftAddress: "address_to_contact",
	}
	mockClient.On("StartRaft", mock.Anything, mock.Anything, mock.Anything).Return(startResp, nil)

	host, _ := os.Hostname()
	cc := &copyCatImpl{
		myAddress:  host + ":" + strconv.Itoa(5599),
		membership: mockMembership,
		newCopyCatClientFunc: func(*sync.Map, string) (pb.CopyCatServiceClient, error) {
			return mockClient, nil
		},
		newInteractiveRaftBackendFunc: func(config *Config, peers []pb.Peer, provider SnapshotProvider) (*raftBackend, error) {
			return &raftBackend{
				proposeChan: make(chan []byte),
				commitChan:  make(chan []byte),
				errorChan:   make(chan error),
			}, nil
		},
		logger: log.WithFields(log.Fields{
			"test": "TestCopyCatConnectToDataStructureNew",
		}),
	}

	myDataStructureId := randomRaftId()
	proposeCh, commitCh, errorCh, snapshotConsumer := cc.ConnectToDataStructure(myDataStructureId, func() ([]byte, error) { return make([]byte, 0), nil })

	assert.NotNil(t, proposeCh)
	assert.NotNil(t, commitCh)
	assert.NotNil(t, errorCh)
	assert.NotNil(t, snapshotConsumer)
	mockMembership.AssertNumberOfCalls(t, "peersForDataStructureId", 1)
	mockMembership.AssertNumberOfCalls(t, "addDataStructureToRaftIdMapping", 1)
	mockMembership.AssertNumberOfCalls(t, "getAllMetadata", 1)
	mockMembership.AssertNumberOfCalls(t, "getAddr", 3)
	mockClient.AssertNumberOfCalls(t, "StartRaft", 3)
}

type mockClient struct {
	mock.Mock
}

func (mc *mockClient) StartRaft(ctx context.Context, in *pb.StartRaftRequest, opts ...grpc.CallOption) (*pb.StartRaftResponse, error) {
	args := mc.Called(ctx, in, opts)
	return args.Get(0).(*pb.StartRaftResponse), args.Error(1)
}

func (mc *mockClient) StopRaft(ctx context.Context, in *pb.StopRaftRequest, opts ...grpc.CallOption) (*pb.StopRaftResponse, error) {
	args := mc.Called(ctx, in, opts)
	return args.Get(0).(*pb.StopRaftResponse), args.Error(1)
}

type mockCopyCatMembership struct {
	mock.Mock
}

func (mccm *mockCopyCatMembership) addDataStructureToRaftIdMapping(dataStructureId uint64, raftId uint64) error {
	args := mccm.Called(dataStructureId, raftId)
	return args.Error(0)
}

func (mccm *mockCopyCatMembership) getAddr(tags map[string]string) string {
	args := mccm.Called(tags)
	return args.String(0)
}

func (mccm *mockCopyCatMembership) getAllMetadata() map[uint64]map[string]string {
	args := mccm.Called()
	return args.Get(0).(map[uint64]map[string]string)
}

func (mccm *mockCopyCatMembership) peersForDataStructureId(dataStructureId uint64) []pb.Peer {
	args := mccm.Called(dataStructureId)
	return args.Get(0).([]pb.Peer)
}

func (mccm *mockCopyCatMembership) stop() error {
	args := mccm.Called()
	return args.Error(0)
}
