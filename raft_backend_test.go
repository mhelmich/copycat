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
	"testing"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestRaftBackendBasic(t *testing.T) {
	fakeTransport := newFakeTransport()
	log.SetLevel(log.DebugLevel)

	// config1 := DefaultConfig()
	// config1.CopyCatDataDir = "./test-TestRaftBackendBasic-" + uint64ToString(randomRaftId()) + "/"
	// err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	// assert.Nil(t, err)
	// config1.CopyCatPort = config1.CopyCatPort + 33333
	// config1.raftTransport = fakeTransport
	// config1.logger = log.WithFields(log.Fields{
	// 	"raft1": "raft1",
	// })
	// detachedBackend1, err := newDetachedRaftBackendWithId(randomRaftId(), config1)
	// assert.Nil(t, err)
	// fakeTransport.add(detachedBackend1)
	// assert.NotNil(t, detachedBackend1)
	//
	// config2 := DefaultConfig()
	// config2.CopyCatDataDir = "./test-TestRaftBackendBasic-" + uint64ToString(randomRaftId()) + "/"
	// err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	// assert.Nil(t, err)
	// config2.CopyCatPort = config1.CopyCatPort + 10000
	// config2.raftTransport = fakeTransport
	// config2.logger = log.WithFields(log.Fields{
	// 	"raft2": "raft2",
	// })
	// detachedBackend2, err := newDetachedRaftBackendWithId(randomRaftId(), config2)
	// assert.Nil(t, err)
	// fakeTransport.add(detachedBackend2)
	// assert.NotNil(t, detachedBackend2)

	config3 := DefaultConfig()
	config3.CopyCatDataDir = "./test-TestRaftBackendBasic-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config3.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	config3.CopyCatPort = config3.CopyCatPort + 33333
	config3.raftTransport = fakeTransport
	config3.logger = log.WithFields(log.Fields{
		"raft3": "raft3",
	})
	// peers3 := make([]pb.Peer, 2)
	// peers3[0] = pb.Peer{
	// 	Id:          detachedBackend1.raftId,
	// 	RaftAddress: config1.hostname + ":" + strconv.Itoa(config1.CopyCatPort),
	// }
	// peers3[1] = pb.Peer{
	// 	Id:          detachedBackend2.raftId,
	// 	RaftAddress: config2.hostname + ":" + strconv.Itoa(config2.CopyCatPort),
	// }
	interactiveBackend, err := newInteractiveRaftBackend(config3, nil, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	fakeTransport.add(interactiveBackend)
	assert.NotNil(t, interactiveBackend)
	time.Sleep(1 * time.Second)
	consumeAndPrintEvents(interactiveBackend)
	assert.NotNil(t, interactiveBackend.raftNode)
	interactiveBackend.raftNode.Campaign(context.TODO())

	interactiveBackend.proposeChan <- []byte("hello")
	// interactiveBackend.proposeChan <- []byte("world")

	time.Sleep(1 * time.Second)

	// detachedBackend1.stop()
	// detachedBackend2.stop()
	interactiveBackend.stop()

	// err = os.RemoveAll(config1.CopyCatDataDir)
	// assert.Nil(t, err)
	// err = os.RemoveAll(config2.CopyCatDataDir)
	// assert.Nil(t, err)
	err = os.RemoveAll(config3.CopyCatDataDir)
	assert.Nil(t, err)
}

func consumeAndPrintEvents(rb *raftBackend) {
	if rb.isInteractive {
		go func() {
			for {
				select {
				case c := <-rb.commitChan:
					log.Debugf("Committed %v", c)
				case e := <-rb.errorChan:
					log.Debugf("Error %v", e)
				}
			}
		}()
	}
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
		log.Infof("Sending message from %d to %d", msg.From, msg.To)
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
