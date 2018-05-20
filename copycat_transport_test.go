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
	"testing"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTransportBasic(t *testing.T) {
	config := DefaultConfig()
	config.CopyCatDataDir = "./test-TestTransportBasic-" + uint64ToString(randomRaftId())
	err := os.MkdirAll(config.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)

	m := &membership{
		memberIdToTags:           make(map[uint64]map[string]string),
		raftIdToAddress:          make(map[uint64]string),
		dataStructureIdToRaftIds: make(map[uint64]map[uint64]bool),
		logger: log.WithFields(log.Fields{
			"test": "TestTransportBasic",
		}),
	}

	transport, err := newTransport(config, m)
	assert.Nil(t, err)

	transport.stop()
	err = os.RemoveAll(config.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestTransportSendReceiveMessages(t *testing.T) {
	m := &membership{
		memberIdToTags:           make(map[uint64]map[string]string),
		raftIdToAddress:          make(map[uint64]string),
		dataStructureIdToRaftIds: make(map[uint64]map[uint64]bool),
		logger: log.WithFields(log.Fields{
			"test": "TestTransportSendReceiveMessages",
		}),
	}

	config1 := DefaultConfig()
	config1.CopyCatPort += 10000
	config1.logger = m.logger
	config1.CopyCatDataDir = "./test-TestTransportSendReceiveMessages-" + uint64ToString(randomRaftId())
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	sender, err := newTransport(config1, m)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.CopyCatPort = config1.CopyCatPort + 10000
	config1.logger = m.logger
	config2.CopyCatDataDir = "./test-TestTransportSendReceiveMessages-" + uint64ToString(randomRaftId())
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	receiver, err := newTransport(config2, m)
	assert.Nil(t, err)

	raftId := randomRaftId()
	msgs := make([]raftpb.Message, 1)
	msgs[0] = raftpb.Message{
		To:    raftId,
		Index: uint64(999),
	}
	m.raftIdToAddress[raftId] = receiver.config.hostname + ":" + strconv.Itoa(receiver.config.CopyCatPort)
	mockBackend := new(mockRaftBackend)
	mockBackend.On("step", mock.MatchedBy(func(ctx context.Context) bool { return true }), msgs[0]).Return(nil)
	receiver.raftBackends[raftId] = mockBackend

	// run test
	sender.sendMessages(msgs)
	mockBackend.AssertNumberOfCalls(t, "step", 1)

	sender.stop()
	receiver.stop()
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestTransportStartStopRaft(t *testing.T) {
	config := DefaultConfig()
	config.CopyCatDataDir = "./test-TestTransportStartStopRaft-" + uint64ToString(randomRaftId())

	mockTransport := new(mockRaftTransport)
	mockTransport.On("sendMessages", mock.Anything).Return()
	config.raftTransport = mockTransport

	mockBackend := new(mockRaftBackend)
	mockBackend.On("step", mock.Anything, mock.Anything).Return(nil)
	mockBackend.On("stop").Return()

	mockMembership := new(mockTransportMembership)
	mockMembership.On("addDataStructureToRaftIdMapping", mock.Anything, mock.Anything).Return(nil)

	transport := &copyCatTransport{
		config:       config,
		raftBackends: make(map[uint64]transportRaftBackend),
		membership:   mockMembership,
		newRaftBackendFunc: func(uint64, *Config) (transportRaftBackend, error) {
			return mockBackend, nil
		},
		logger: log.WithFields(log.Fields{
			"test": "TestTransportStartStopRaft",
		}),
	}

	respStart, err := transport.StartRaft(context.TODO(), &pb.StartRaftRequest{})
	assert.Nil(t, err)
	assert.NotNil(t, respStart.RaftId)
	assert.NotNil(t, respStart.RaftAddress)
	assert.Equal(t, config.hostname+":"+strconv.Itoa(config.CopyCatPort), respStart.RaftAddress)
	rb, ok := transport.raftBackends[respStart.RaftId]
	assert.NotNil(t, rb)
	assert.True(t, ok)

	respStop, err := transport.StopRaft(context.TODO(), &pb.StopRaftRequest{RaftId: respStart.RaftId})
	assert.Nil(t, err)
	assert.NotNil(t, respStop)

	mockBackend.AssertNumberOfCalls(t, "step", 0)
	mockBackend.AssertNumberOfCalls(t, "stop", 1)
	mockMembership.AssertNumberOfCalls(t, "addDataStructureToRaftIdMapping", 1)
	err = os.RemoveAll(config.CopyCatDataDir)
	assert.Nil(t, err)
}

type mockRaftBackend struct {
	mock.Mock
}

func (rb *mockRaftBackend) step(ctx context.Context, msg raftpb.Message) error {
	args := rb.Called(ctx, msg)
	return args.Error(0)
}

func (rb *mockRaftBackend) stop() {
	rb.Called()
}

type mockRaftTransport struct {
	mock.Mock
}

func (t *mockRaftTransport) sendMessages(msgs []raftpb.Message) {
	t.Called(msgs)
}

type mockTransportMembership struct {
	mock.Mock
}

func (mtm *mockTransportMembership) addDataStructureToRaftIdMapping(dataStructureId uint64, raftId uint64) error {
	args := mtm.Called(dataStructureId, raftId)
	return args.Error(0)
}

func (mtm *mockTransportMembership) getAddressForRaftId(raftId uint64) string {
	args := mtm.Called(raftId)
	return args.String(0)
}

func (mtm *mockTransportMembership) getAddressesForDataStructureId(dataStructureId uint64) []string {
	args := mtm.Called(dataStructureId)
	o := args.Get(0)
	if o != nil {
		return o.([]string)
	}

	return nil
}
