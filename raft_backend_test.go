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
	"strconv"
	"testing"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestRaftBackendBasic(t *testing.T) {
	fakeTransport := newFakeTransport()

	config1 := DefaultConfig()
	config1.CopyCatDataDir = "./test-TestRaftBackendBasic-" + uint64ToString(randomRaftId()) + "/"
	config1.GossipPort = config1.GossipPort + 22222
	config1.CopyCatPort = config1.CopyCatPort + 22222
	config1.raftTransport = fakeTransport
	detachedBackend, err := newDetachedRaftBackendWithId(randomRaftId(), config1)
	assert.Nil(t, err)
	fakeTransport.add(detachedBackend)
	assert.NotNil(t, detachedBackend)

	config2 := DefaultConfig()
	config2.CopyCatDataDir = "./test-TestRaftBackendBasic-" + uint64ToString(randomRaftId()) + "/"
	config2.GossipPort = config1.GossipPort + 10000
	config2.CopyCatPort = config1.CopyCatPort + 10000
	config2.raftTransport = fakeTransport
	peers := make([]pb.Peer, 1)
	peers[0] = pb.Peer{
		Id:          detachedBackend.raftId,
		RaftAddress: config1.hostname + ":" + strconv.Itoa(config1.CopyCatPort),
	}
	interactiveBackend, err := newInteractiveRaftBackend(config2, peers, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	fakeTransport.add(interactiveBackend)
	assert.NotNil(t, interactiveBackend)
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		backends: make(map[uint64]*raftBackend),
	}
}

type fakeTransport struct {
	backends map[uint64]*raftBackend
}

func (ft *fakeTransport) add(rb *raftBackend) {
	ft.backends[rb.raftId] = rb
}

func (ft *fakeTransport) sendMessages(msgs []raftpb.Message) {
	for _, msg := range msgs {
		rb, ok := ft.backends[msg.To]
		if !ok {
			log.Panicf("You didn't set the test correctly! Backend with id %d doesn't exist!", msg.To)
		}

		err := rb.step(context.TODO(), msg)
		if err != nil {
			log.Errorf("Error stepping in raft %d: %s", msg.To, err.Error())
		}
	}
}
