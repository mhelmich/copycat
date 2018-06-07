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
	"testing"

	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestTransportBasic(t *testing.T) {
	config := DefaultConfig()
	config.CopyCatDataDir = "./test-TestTransportBasic-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)

	m := new(mockMembershipProxy)
	transport, err := newTransport(config, m)
	assert.Nil(t, err)

	transport.stop()
	err = os.RemoveAll(config.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestTransportSendReceiveMessages(t *testing.T) {
	// create the mock here
	// mock out the methods later
	// there's a chicken and egg problem where I need the proxy
	m := new(mockMembershipProxy)

	config1 := DefaultConfig()
	config1.CopyCatPort += 10000
	config1.logger = log.WithFields(log.Fields{})
	config1.CopyCatDataDir = "./test-TestTransportSendReceiveMessages-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	sender, err := newTransport(config1, m)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.CopyCatPort = config1.CopyCatPort + 10000
	config1.logger = log.WithFields(log.Fields{})
	config2.CopyCatDataDir = "./test-TestTransportSendReceiveMessages-" + uint64ToString(randomRaftId()) + "/"
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

	conn, err := grpc.Dial("127.0.0.1:"+strconv.Itoa(config2.CopyCatPort), grpc.WithInsecure())
	assert.Nil(t, err)
	client := pb.NewRaftTransportServiceClient(conn)

	m.On("getRaftTransportServiceClientForRaftId", mock.Anything).Return(client, nil)
	m.On("stepRaft", mock.Anything, mock.Anything).Return(nil)

	// run test
	sendingResults := sender.sendMessages(msgs)
	assert.Nil(t, sendingResults)
	m.AssertNumberOfCalls(t, "getRaftTransportServiceClientForRaftId", 1)
	m.AssertNumberOfCalls(t, "stepRaft", 1)

	sender.stop()
	receiver.stop()
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestTransportReportFailures(t *testing.T) {
	failedMessage := uint64(99)
	snapMessage := uint64(88)
	succeededNonSnapMessage := uint64(111)

	mockStream := new(mockRaftTransportService_SendClient)
	// fails for all nodes to failed message
	mockStream.On("Send", mock.MatchedBy(func(req *pb.SendReq) bool { return req.Message.To == failedMessage })).Return(fmt.Errorf("BOOOM!"))
	// succeeds and hence needs all subsequent calls as well
	mockStream.On("Send", mock.MatchedBy(func(req *pb.SendReq) bool { return req.Message.To == snapMessage })).Return(nil)
	mockStream.On("Send", mock.MatchedBy(func(req *pb.SendReq) bool { return req.Message.To == succeededNonSnapMessage })).Return(nil)
	mockStream.On("Recv").Return(&pb.SendResp{}, nil)
	mockStream.On("CloseSend").Return(nil)

	mockClient := new(mockRaftTransportServiceClient)
	mockClient.On("Send", mock.Anything).Return(mockStream, nil)
	mockMemberProxy := new(mockMembershipProxy)
	mockMemberProxy.On("getRaftTransportServiceClientForRaftId", mock.Anything).Return(mockClient, nil)

	transport := &copyCatTransport{
		membership:      mockMemberProxy,
		errorLogLimiter: rate.NewLimiter(10.0, 10),
		logger:          log.WithFields(log.Fields{}),
	}

	msgs := make([]raftpb.Message, 2)
	msgs[0] = raftpb.Message{
		To: failedMessage,
	}
	msgs[1] = raftpb.Message{
		To:   snapMessage,
		Type: raftpb.MsgSnap,
	}
	results := transport.sendMessages(msgs)
	assert.NotNil(t, results)
	assert.Equal(t, failedMessage, results.failedMessages[0].To)
	assert.Equal(t, snapMessage, results.succeededSnapshotMessages[0].To)

	msgs = make([]raftpb.Message, 1)
	msgs[0] = raftpb.Message{
		To: succeededNonSnapMessage,
	}
	results = transport.sendMessages(msgs)
	assert.Nil(t, results)
}

func TestTransportStartStopRaft(t *testing.T) {
	m := new(mockMembershipProxy)
	m.On("newDetachedRaftBackend", mock.Anything, mock.Anything, mock.Anything).Return(nil, nil)
	m.On("stopRaft", mock.Anything).Return(nil)

	config := DefaultConfig()
	config.CopyCatDataDir = "./test-TestTransportStartStopRaft-" + uint64ToString(randomRaftId()) + "/"

	mockTransport := new(mockRaftTransport)
	config.raftTransport = mockTransport

	transport := &copyCatTransport{
		config:     config,
		membership: m,
		myAddress:  config.Hostname + ":" + strconv.Itoa(config.CopyCatPort),
		logger: log.WithFields(log.Fields{
			"test": "TestTransportStartStopRaft",
		}),
	}

	respStart, err := transport.StartRaft(context.TODO(), &pb.StartRaftRequest{})
	assert.Nil(t, err)
	assert.NotNil(t, respStart.RaftId)
	assert.NotNil(t, respStart.RaftAddress)
	assert.Equal(t, config.Hostname+":"+strconv.Itoa(config.CopyCatPort), respStart.RaftAddress)

	respStop, err := transport.StopRaft(context.TODO(), &pb.StopRaftRequest{RaftId: respStart.RaftId})
	assert.Nil(t, err)
	assert.NotNil(t, respStop)

	m.AssertNumberOfCalls(t, "newDetachedRaftBackend", 1)
	m.AssertNumberOfCalls(t, "stopRaft", 1)
	err = os.RemoveAll(config.CopyCatDataDir)
	assert.Nil(t, err)
}
