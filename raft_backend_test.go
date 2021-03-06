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
	"crypto/rand"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestRaftBackendBasic(t *testing.T) {
	fakeTransport := newFakeTransport()

	config1 := DefaultConfig()
	config1.CopyCatDataDir = "./test-TestRaftBackendBasic-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config1.CopyCatPort = config1.CopyCatPort + 22222
	config1.raftTransport = fakeTransport
	config1.logger = log.WithFields(log.Fields{
		"raft1": "raft1",
	})
	config2 := DefaultConfig()
	config2.CopyCatDataDir = "./test-TestRaftBackendBasic-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config2.CopyCatPort = config1.CopyCatPort + 1111
	config2.raftTransport = fakeTransport
	config2.logger = log.WithFields(log.Fields{
		"raft2": "raft2",
	})
	allPeers := make([]*pb.RaftPeer, 2)
	allPeers[0] = &pb.RaftPeer{
		RaftId:      randomRaftId(),
		PeerAddress: config1.address(),
	}
	allPeers[1] = &pb.RaftPeer{
		RaftId:      randomRaftId(),
		PeerAddress: config2.address(),
	}

	detachedBackend1, err := newDetachedRaftBackendWithIdAndPeers(allPeers[0].RaftId, config1, allPeers)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend1)
	assert.NotNil(t, detachedBackend1)

	detachedBackend2, err := newDetachedRaftBackendWithIdAndPeers(allPeers[1].RaftId, config2, allPeers)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend2)
	assert.NotNil(t, detachedBackend2)

	config3 := DefaultConfig()
	config3.CopyCatDataDir = "./test-TestRaftBackendBasic-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config3.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config3.CopyCatPort = config2.CopyCatPort + 1111
	config3.raftTransport = fakeTransport
	config3.logger = log.WithFields(log.Fields{
		"raft3": "raft3",
	})
	interactiveBackend, err := newInteractiveRaftBackendForExistingGroup(config3, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	fakeTransport.add(interactiveBackend)
	assert.NotNil(t, interactiveBackend)
	assert.NotNil(t, interactiveBackend.raftNode)
	detachedBackend1.addRaftToMyGroup(context.TODO(), interactiveBackend.raftId)

	hello := []byte("hello")
	world := []byte("world")
	interactiveBackend.proposeChan <- hello
	bites := <-interactiveBackend.commitChan
	assert.Equal(t, hello, bites)
	interactiveBackend.proposeChan <- world
	bites = <-interactiveBackend.commitChan
	assert.Equal(t, world, bites)

	detachedBackend1.stop()
	detachedBackend2.stop()
	interactiveBackend.stop()

	err = removeAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = removeAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
	err = removeAll(config3.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestRaftBackendAddNewRaftToExistingGroup(t *testing.T) {
	fakeTransport := newFakeTransport()

	config1 := DefaultConfig()
	config1.CopyCatDataDir = "./test-TestRaftBackendAddNewRaftToExistingGroup-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config1.CopyCatPort = config1.CopyCatPort + 22222
	config1.raftTransport = fakeTransport
	config1.logger = log.WithFields(log.Fields{
		"raft1": "raft1",
	})
	config2 := DefaultConfig()
	config2.CopyCatDataDir = "./test-TestRaftBackendAddNewRaftToExistingGroup-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config2.CopyCatPort = config1.CopyCatPort + 1111
	config2.raftTransport = fakeTransport
	config2.logger = log.WithFields(log.Fields{
		"raft2": "raft2",
	})
	allPeers := make([]*pb.RaftPeer, 2)
	allPeers[0] = &pb.RaftPeer{
		RaftId:      randomRaftId(),
		PeerAddress: config1.address(),
	}
	allPeers[1] = &pb.RaftPeer{
		RaftId:      randomRaftId(),
		PeerAddress: config2.address(),
	}

	detachedBackend1, err := newDetachedRaftBackendWithIdAndPeers(allPeers[0].RaftId, config1, allPeers)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend1)
	assert.NotNil(t, detachedBackend1)

	detachedBackend2, err := newDetachedRaftBackendWithIdAndPeers(allPeers[1].RaftId, config2, allPeers)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend2)
	assert.NotNil(t, detachedBackend2)

	config3 := DefaultConfig()
	config3.CopyCatDataDir = "./test-TestRaftBackendAddNewRaftToExistingGroup-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config3.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config3.CopyCatPort = config2.CopyCatPort + 1111
	config3.raftTransport = fakeTransport
	config3.logger = log.WithFields(log.Fields{
		"raft3": "raft3",
	})
	interactiveBackend1, err := newInteractiveRaftBackendForExistingGroup(config3, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	fakeTransport.add(interactiveBackend1)
	assert.NotNil(t, interactiveBackend1)
	assert.NotNil(t, interactiveBackend1.raftNode)
	detachedBackend2.addRaftToMyGroup(context.TODO(), interactiveBackend1.raftId)

	hello := []byte("hello")
	world := []byte("world")
	interactiveBackend1.proposeChan <- hello
	bites := <-interactiveBackend1.commitChan
	assert.Equal(t, hello, bites)
	interactiveBackend1.proposeChan <- world
	bites = <-interactiveBackend1.commitChan
	assert.Equal(t, world, bites)

	config4 := DefaultConfig()
	config4.CopyCatDataDir = "./test-TestRaftBackendAddNewRaftToExistingGroup-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config4.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config4.CopyCatPort = config3.CopyCatPort + 1111
	config4.raftTransport = fakeTransport
	config4.logger = log.WithFields(log.Fields{
		"raft4": "raft4",
	})

	interactiveBackend2, err := newInteractiveRaftBackendForExistingGroup(config4, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	fakeTransport.add(interactiveBackend2)
	assert.NotNil(t, interactiveBackend2)
	assert.NotNil(t, interactiveBackend2.raftNode)
	interactiveBackend1.proposeConfChangeChan <- raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddNode,
		NodeID: interactiveBackend2.raftId,
	}

	bites = <-interactiveBackend2.commitChan
	assert.Equal(t, hello, bites)
	bites = <-interactiveBackend2.commitChan
	assert.Equal(t, world, bites)

	detachedBackend1.stop()
	detachedBackend2.stop()
	interactiveBackend1.stop()
	interactiveBackend2.stop()

	err = removeAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = removeAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
	err = removeAll(config3.CopyCatDataDir)
	assert.Nil(t, err)
	err = removeAll(config4.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestRaftBackendTriggerSnapshot(t *testing.T) {
	dir := "./test-TestTriggerSnapshot-" + uint64ToString(randomRaftId()) + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)

	rb := &raftBackend{
		appliedIndex:      uint64(99),
		snapshotIndex:     uint64(98),
		snapshotFrequency: uint64(0),
		snapshotProvider: func() ([]byte, error) {
			bites := make([]byte, 1024)
			rand.Read(bites)
			return bites, nil
		},
		store: store,
	}

	snap, err := store.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), snap.Metadata.Index)
	rb.maybeTriggerSnapshot()
	snap, err = store.Snapshot()
	assert.Nil(t, err)
	assert.Equal(t, rb.appliedIndex, snap.Metadata.Index)
	assert.Equal(t, 1024, len(snap.Data))

	store.close()
	removeAll(dir)
}

func TestRaftBackendPublishSnaphot(t *testing.T) {
	dir := "./test-TestPublishSnaphot-" + uint64ToString(randomRaftId()) + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)

	mockTransport := new(mockRaftTransport)
	mockTransport.On("sendMessages", mock.Anything).Return(nil)
	mockRaftNode := new(mockRaftNode)
	mockRaftNode.On("Advance").Return()

	mockBites := make([]byte, 1024)
	rand.Read(mockBites)
	mockSnapshot := raftpb.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index: uint64(99),
		},
		Data: mockBites,
	}
	mockReady := raft.Ready{
		Snapshot: mockSnapshot,
	}

	commitCh := make(chan []byte, 1)

	rb := &raftBackend{
		raftId:            randomRaftId(),
		isInteractive:     true,
		commitChan:        commitCh,
		appliedIndex:      uint64(0),
		snapshotIndex:     uint64(0),
		snapshotFrequency: uint64(1000),
		transport:         mockTransport,
		raftNode:          mockRaftNode,
		logger:            log.WithFields(log.Fields{}),
		store:             store,
	}

	ok := rb.procesReady(mockReady)
	assert.True(t, ok)

	assert.Equal(t, uint64(99), rb.appliedIndex)
	assert.Equal(t, uint64(99), rb.snapshotIndex)
	commit, ok := <-commitCh
	assert.True(t, ok)
	assert.Nil(t, commit)

	snap, err := store.Snapshot()
	assert.Nil(t, err)
	assert.NotNil(t, snap.Data)
	assert.Equal(t, 1024, len(snap.Data))

	bites, err := rb.snapshot()
	assert.Nil(t, err)
	assert.Equal(t, 1024, len(bites))

	mockTransport.AssertNumberOfCalls(t, "sendMessages", 1)
	mockRaftNode.AssertNumberOfCalls(t, "Advance", 1)

	store.close()
	removeAll(dir)
}

func TestRaftBackendAddRaftToGroup(t *testing.T) {
	node := new(mockRaftNode)
	node.On("ProposeConfChange", mock.Anything, mock.MatchedBy(func(cc raftpb.ConfChange) bool { return cc.ID == uint64(99) && cc.NodeID == uint64(11) })).Return(nil)
	backend := &raftBackend{
		raftNode:           node,
		latestConfChangeId: uint64(98),
	}

	backend.addRaftToMyGroup(context.TODO(), uint64(11))
	node.AssertNumberOfCalls(t, "ProposeConfChange", 1)
}

func TestRaftBackendGracefulShutdownFailover(t *testing.T) {
	fakeTransport := newFakeTransport()

	config1 := DefaultConfig()
	config1.CopyCatDataDir = "./test-TestRaftBackendGracefulShutdownFailover-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config1.CopyCatPort = config1.CopyCatPort + 22222
	config1.raftTransport = fakeTransport
	config1.logger = log.WithFields(log.Fields{
		"raft1": "raft1",
	})
	config2 := DefaultConfig()
	config2.CopyCatDataDir = "./test-TestRaftBackendGracefulShutdownFailover-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config2.CopyCatPort = config1.CopyCatPort + 1111
	config2.raftTransport = fakeTransport
	config2.logger = log.WithFields(log.Fields{
		"raft2": "raft2",
	})
	config3 := DefaultConfig()
	config3.CopyCatDataDir = "./test-TestRaftBackendSplitBrainFailover-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config3.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config3.CopyCatPort = config2.CopyCatPort + 1111
	config3.raftTransport = fakeTransport
	config3.logger = log.WithFields(log.Fields{
		"raft3": "raft3",
	})
	allPeers := make([]*pb.RaftPeer, 3)
	allPeers[0] = &pb.RaftPeer{
		RaftId:      randomRaftId(),
		PeerAddress: config1.address(),
	}
	allPeers[1] = &pb.RaftPeer{
		RaftId:      randomRaftId(),
		PeerAddress: config2.address(),
	}
	allPeers[2] = &pb.RaftPeer{
		RaftId:      randomRaftId(),
		PeerAddress: config3.address(),
	}

	detachedBackend1, err := newDetachedRaftBackendWithIdAndPeers(allPeers[0].RaftId, config1, allPeers)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend1)
	assert.NotNil(t, detachedBackend1)

	detachedBackend2, err := newDetachedRaftBackendWithIdAndPeers(allPeers[1].RaftId, config2, allPeers)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend2)
	assert.NotNil(t, detachedBackend2)

	detachedBackend3, err := newDetachedRaftBackendWithIdAndPeers(allPeers[2].RaftId, config3, allPeers)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend3)
	assert.NotNil(t, detachedBackend3)

	config4 := DefaultConfig()
	config4.CopyCatDataDir = "./test-TestRaftBackendGracefulShutdownFailover-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config4.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config4.CopyCatPort = config3.CopyCatPort + 1111
	config4.raftTransport = fakeTransport
	config4.logger = log.WithFields(log.Fields{
		"raft4": "raft4",
	})
	interactiveBackend, err := newInteractiveRaftBackendForExistingGroup(config4, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	fakeTransport.add(interactiveBackend)
	assert.NotNil(t, interactiveBackend)
	assert.NotNil(t, interactiveBackend.raftNode)
	detachedBackend1.addRaftToMyGroup(context.TODO(), interactiveBackend.raftId)

	hello := []byte("hello")
	world := []byte("world")
	interactiveBackend.proposeChan <- hello
	bites := <-interactiveBackend.commitChan
	assert.Equal(t, hello, bites)
	interactiveBackend.proposeChan <- world
	bites = <-interactiveBackend.commitChan
	assert.Equal(t, world, bites)
	assert.Equal(t, 3, len(detachedBackend3.confState.Nodes))
	assert.Equal(t, 1, len(detachedBackend3.confState.Learners))

	master := findLeaderBackend(detachedBackend1, detachedBackend2, detachedBackend3, interactiveBackend)
	assert.NotNil(t, master)
	followers := findAllFollowers(detachedBackend1, detachedBackend2, detachedBackend3, interactiveBackend)
	assert.Equal(t, 3, len(followers))
	master.stop()
	log.Infof("Stopped leader: %d %x", master.raftId, master.raftId)

	// HACK
	// elections happen after a second (10 ticks à 100ms +/- random) or so
	time.Sleep(2 * time.Second)

	master = findLeaderBackend(detachedBackend1, detachedBackend2, detachedBackend3, interactiveBackend)
	assert.NotNil(t, master)
	followers = findAllFollowers(detachedBackend1, detachedBackend2, detachedBackend3, interactiveBackend)
	assert.Equal(t, 3, len(followers))

	for _, b := range followers {
		b.stop()
	}

	err = removeAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = removeAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
	err = removeAll(config3.CopyCatDataDir)
	assert.Nil(t, err)
	err = removeAll(config4.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestRaftBackendSplitBrainFailover(t *testing.T) {
	fakeTransport := newFakeTransport()

	config1 := DefaultConfig()
	config1.CopyCatDataDir = "./test-TestRaftBackendSplitBrainFailover-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config1.CopyCatPort = config1.CopyCatPort + 22222
	config1.raftTransport = fakeTransport
	config1.logger = log.WithFields(log.Fields{
		"raft1": "raft1",
	})
	config2 := DefaultConfig()
	config2.CopyCatDataDir = "./test-TestRaftBackendSplitBrainFailover-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config2.CopyCatPort = config1.CopyCatPort + 1111
	config2.raftTransport = fakeTransport
	config2.logger = log.WithFields(log.Fields{
		"raft2": "raft2",
	})
	config3 := DefaultConfig()
	config3.CopyCatDataDir = "./test-TestRaftBackendSplitBrainFailover-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config3.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config3.CopyCatPort = config2.CopyCatPort + 1111
	config3.raftTransport = fakeTransport
	config3.logger = log.WithFields(log.Fields{
		"raft3": "raft3",
	})
	allPeers := make([]*pb.RaftPeer, 3)
	allPeers[0] = &pb.RaftPeer{
		RaftId:      randomRaftId(),
		PeerAddress: config1.address(),
	}
	allPeers[1] = &pb.RaftPeer{
		RaftId:      randomRaftId(),
		PeerAddress: config2.address(),
	}
	allPeers[2] = &pb.RaftPeer{
		RaftId:      randomRaftId(),
		PeerAddress: config3.address(),
	}

	detachedBackend1, err := newDetachedRaftBackendWithIdAndPeers(allPeers[0].RaftId, config1, allPeers)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend1)
	assert.NotNil(t, detachedBackend1)

	detachedBackend2, err := newDetachedRaftBackendWithIdAndPeers(allPeers[1].RaftId, config2, allPeers)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend2)
	assert.NotNil(t, detachedBackend2)

	detachedBackend3, err := newDetachedRaftBackendWithIdAndPeers(allPeers[2].RaftId, config3, allPeers)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend3)
	assert.NotNil(t, detachedBackend3)

	time.Sleep(1 * time.Second)
	assert.Equal(t, 3, len(detachedBackend3.confState.Nodes))
	assert.Equal(t, 0, len(detachedBackend3.confState.Learners))

	config4 := DefaultConfig()
	config4.CopyCatDataDir = "./test-TestRaftBackendSplitBrainFailover-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config4.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config4.CopyCatPort = config3.CopyCatPort + 1111
	config4.raftTransport = fakeTransport
	config4.logger = log.WithFields(log.Fields{
		"raft4": "raft4",
	})
	interactiveBackend, err := newInteractiveRaftBackendForExistingGroup(config4, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	fakeTransport.add(interactiveBackend)
	assert.NotNil(t, interactiveBackend)
	assert.NotNil(t, interactiveBackend.raftNode)
	detachedBackend1.addRaftToMyGroup(context.TODO(), interactiveBackend.raftId)

	hello := []byte("hello")
	world := []byte("world")
	interactiveBackend.proposeChan <- hello
	bites := <-interactiveBackend.commitChan
	assert.Equal(t, hello, bites)
	interactiveBackend.proposeChan <- world
	bites = <-interactiveBackend.commitChan
	assert.Equal(t, world, bites)

	assert.Equal(t, 3, len(detachedBackend3.confState.Nodes))
	assert.Equal(t, 1, len(detachedBackend3.confState.Learners))
	log.Warnf("conf state: %s", detachedBackend1.confState.String())

	// HACK
	// elections happen after a second (10 ticks à 100ms +/- random) or so
	var master1 *raftBackend
	for i := 0; i < 20 && master1 == nil; i++ {
		master1 = findLeaderBackend(detachedBackend1, detachedBackend2, detachedBackend3, interactiveBackend)
		time.Sleep(100 * time.Millisecond)
	}
	log.Infof("master %d %x", master1.raftId, master1.raftId)
	assert.NotNil(t, master1)
	followers := findAllFollowers(detachedBackend1, detachedBackend2, detachedBackend3, interactiveBackend)
	for _, rb := range followers {
		log.Infof("follower %d %x", rb.raftId, rb.raftId)
	}
	assert.Equal(t, 3, len(followers))
	fakeTransport.isolateRaft(master1.raftId)
	log.Infof("Isolated leader: %d %x", master1.raftId, master1.raftId)
	master1.stop()
	log.Infof("Stopped leader: %d %x", master1.raftId, master1.raftId)

	// HACK
	// elections happen after a second (10 ticks à 100ms +/- random) or so
	var master2 *raftBackend
	time.Sleep(2000 * time.Millisecond)
	for i := 0; i < 20; i++ {
		if master2 == nil {
			master2 = findLeaderBackend(detachedBackend1, detachedBackend2, detachedBackend3, interactiveBackend)
			time.Sleep(100 * time.Millisecond)
		} else if master1.raftId == master2.raftId {
			master2 = findLeaderBackend(detachedBackend1, detachedBackend2, detachedBackend3, interactiveBackend)
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	log.Infof("master %d %x", master2.raftId, master2.raftId)
	assert.NotNil(t, master2)
	followers = findAllFollowers(detachedBackend1, detachedBackend2, detachedBackend3, interactiveBackend)
	for _, rb := range followers {
		log.Infof("follower %d %x", rb.raftId, rb.raftId)
	}
	assert.Equal(t, 3, len(followers))

	for _, b := range followers {
		b.stop()
	}

	err = removeAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = removeAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
	err = removeAll(config3.CopyCatDataDir)
	assert.Nil(t, err)
	err = removeAll(config4.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestRaftBackendStartWithSnapshot(t *testing.T) {
	mockStore := new(mockStore)
	snapData := make([]byte, 0)
	snap := raftpb.Snapshot{
		Data: snapData,
		Metadata: raftpb.SnapshotMetadata{
			Index: uint64(99),
			Term:  uint64(2),
			ConfState: raftpb.ConfState{
				Nodes: []uint64{uint64(88)},
			},
		},
	}
	mockStore.On("Snapshot").Return(snap, nil)
	mockStore.On("close").Return()

	mockRaftNode := new(mockRaftNode)
	readyCh := make(chan raft.Ready)
	var readOnlyReadyCh <-chan raft.Ready = readyCh
	mockRaftNode.On("Ready").Return(readOnlyReadyCh)
	mockRaftNode.On("Tick").Return()
	mockRaftNode.On("Stop").Return()

	commitCh := make(chan []byte)
	stopCh := make(chan struct{})

	rb := &raftBackend{
		store:         mockStore,
		raftNode:      mockRaftNode,
		isInteractive: true,
		commitChan:    commitCh,
		stopChan:      stopCh,
		logger:        log.WithFields(log.Fields{}),
	}

	go rb.runRaftStateMachine()
	data, ok := <-commitCh
	assert.True(t, ok)
	assert.Nil(t, data)
	assert.Equal(t, uint64(99), rb.snapshotIndex)
	assert.Equal(t, uint64(99), rb.appliedIndex)
	assert.Equal(t, 1, len(rb.confState.Nodes))
	assert.Equal(t, uint64(88), rb.confState.Nodes[0])

	close(stopCh)
}

func findLeaderBackend(backends ...*raftBackend) *raftBackend {
	for _, b := range backends {
		if b.raftNode.Status().RaftState == raft.StateLeader {
			return b
		}
	}
	return nil
}

func findAllFollowers(backends ...*raftBackend) []*raftBackend {
	a := make([]*raftBackend, 0)
	for _, b := range backends {
		if b.raftNode.Status().RaftState == raft.StateFollower {
			a = append(a, b)
		}
	}
	return a
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		backends:   &sync.Map{},
		isolations: &sync.Map{},
	}
}

type fakeTransport struct {
	backends   *sync.Map
	isolations *sync.Map
}

func (ft *fakeTransport) add(rb *raftBackend) {
	ft.backends.Store(rb.raftId, rb)
}

func (ft *fakeTransport) sendMessages(msgs []raftpb.Message) *messageSendingResults {
	var results *messageSendingResults
	for _, msg := range msgs {
		val, ok := ft.backends.Load(msg.To)
		rb := val.(*raftBackend)
		if !ok {
			log.Panicf("You didn't set the test correctly! Backend with id %d doesn't exist!", msg.To)
		}

		err := ft.maybeSend(rb, msg)
		if err != nil {
			log.Errorf("Error stepping in raft %d %x: %s", msg.To, msg.To, err.Error())
			if results == nil {
				results = &messageSendingResults{
					failedMessages: make([]raftpb.Message, 0),
				}
			}
			results.failedMessages = append(results.failedMessages, msg)
		}
	}

	return results
}

func (ft *fakeTransport) maybeSend(rb *raftBackend, msg raftpb.Message) error {
	if msg.To == msg.From {
		return rb.step(context.TODO(), msg)
	}

	_, ok := ft.isolations.Load(msg.To)
	if ok {
		return fmt.Errorf("Can't send to node %d %x", msg.To, msg.To)
	}

	_, ok = ft.isolations.Load(msg.From)
	if ok {
		return fmt.Errorf("Can't receive from node %d %x", msg.To, msg.To)
	}

	return rb.step(context.TODO(), msg)
}

func (ft *fakeTransport) isolateRaft(raftId uint64) {
	ft.isolations.Store(raftId, true)
}
