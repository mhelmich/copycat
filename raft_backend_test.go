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
	"os"
	"strconv"
	"sync"
	"testing"

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
	detachedBackend1, err := newDetachedRaftBackendWithId(randomRaftId(), config1)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend1)
	assert.NotNil(t, detachedBackend1)

	config2 := DefaultConfig()
	config2.CopyCatDataDir = "./test-TestRaftBackendBasic-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config2.CopyCatPort = config1.CopyCatPort + 1111
	config2.raftTransport = fakeTransport
	config2.logger = log.WithFields(log.Fields{
		"raft2": "raft2",
	})
	detachedBackend2, err := newDetachedRaftBackendWithId(randomRaftId(), config2)
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
	peers3 := make([]pb.Peer, 2)
	peers3[0] = pb.Peer{
		Id:          detachedBackend1.raftId,
		RaftAddress: config1.hostname + ":" + strconv.Itoa(config1.CopyCatPort),
	}
	peers3[1] = pb.Peer{
		Id:          detachedBackend2.raftId,
		RaftAddress: config2.hostname + ":" + strconv.Itoa(config2.CopyCatPort),
	}
	interactiveBackend, err := newInteractiveRaftBackend(config3, peers3, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	fakeTransport.add(interactiveBackend)
	assert.NotNil(t, interactiveBackend)
	assert.NotNil(t, interactiveBackend.raftNode)

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

	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.RemoveAll(config3.CopyCatDataDir)
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
	detachedBackend1, err := newDetachedRaftBackendWithId(randomRaftId(), config1)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend1)
	assert.NotNil(t, detachedBackend1)

	config2 := DefaultConfig()
	config2.CopyCatDataDir = "./test-TestRaftBackendAddNewRaftToExistingGroup-" + uint64ToString(randomRaftId()) + "/"
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config2.CopyCatPort = config1.CopyCatPort + 1111
	config2.raftTransport = fakeTransport
	config2.logger = log.WithFields(log.Fields{
		"raft2": "raft2",
	})
	detachedBackend2, err := newDetachedRaftBackendWithId(randomRaftId(), config2)
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
	peers3 := make([]pb.Peer, 2)
	peers3[0] = pb.Peer{
		Id:          detachedBackend1.raftId,
		RaftAddress: config1.hostname + ":" + strconv.Itoa(config1.CopyCatPort),
	}
	peers3[1] = pb.Peer{
		Id:          detachedBackend2.raftId,
		RaftAddress: config2.hostname + ":" + strconv.Itoa(config2.CopyCatPort),
	}
	interactiveBackend1, err := newInteractiveRaftBackend(config3, peers3, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	fakeTransport.add(interactiveBackend1)
	assert.NotNil(t, interactiveBackend1)
	assert.NotNil(t, interactiveBackend1.raftNode)

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

	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.RemoveAll(config3.CopyCatDataDir)
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
	os.RemoveAll(dir)
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
	os.RemoveAll(dir)
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

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		backends: &sync.Map{},
	}
}

type fakeTransport struct {
	backends *sync.Map
}

func (ft *fakeTransport) add(rb *raftBackend) {
	ft.backends.Store(rb.raftId, rb)
}

func (ft *fakeTransport) sendMessages(msgs []raftpb.Message) *messageSendingResults {
	for _, msg := range msgs {
		val, ok := ft.backends.Load(msg.To)
		rb := val.(*raftBackend)
		if !ok {
			log.Panicf("You didn't set the test correctly! Backend with id %d doesn't exist!", msg.To)
		}

		err := rb.step(context.TODO(), msg)
		if err != nil {
			log.Errorf("Error stepping in raft %d: %s", msg.To, err.Error())
		}
	}

	return nil
}
