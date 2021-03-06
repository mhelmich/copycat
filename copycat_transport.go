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
	"io"
	"net"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
)

type copyCatTransport struct {
	config          *Config
	grpcServer      *grpc.Server
	myAddress       string
	membership      membershipProxy
	errorLogLimiter *rate.Limiter
	logger          *log.Entry
}

func newTransport(config *Config, membership membershipProxy) (*copyCatTransport, error) {
	logger := config.logger.WithFields(log.Fields{
		"component": "copycat_transport",
	})

	myAddress := fmt.Sprintf("%s:%d", config.Hostname, config.CopyCatPort)
	lis, err := net.Listen("tcp", myAddress)
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
		return nil, err
	}

	transport := &copyCatTransport{
		config:          config,
		grpcServer:      grpc.NewServer(),
		myAddress:       myAddress,
		membership:      membership,
		errorLogLimiter: rate.NewLimiter(1.0, 1),
		logger:          logger,
	}

	pb.RegisterCopyCatServiceServer(transport.grpcServer, transport)
	pb.RegisterRaftTransportServiceServer(transport.grpcServer, transport)
	go transport.grpcServer.Serve(lis)

	return transport, nil
}

//////////////////////////////////////////
////////////////////////////////
// SECTION FOR THE COPYCAT SERVICE

func (t *copyCatTransport) StartRaft(ctx context.Context, in *pb.StartRaftRequest) (*pb.StartRaftResponse, error) {
	// A raft backend is started in join mode but without specifying other peers.
	// It will just sit there and do nothing until a leader with higher term contacts it.
	// After that the new backend will try to respond to the messages it has been receiving and join the cluster.
	dsId, _ := parseIdFromProto(in.DataStructureId)
	_, err := t.membership.newDetachedRaftBackend(dsId, in.RaftIdToUse, t.config, in.AllRaftPeers)
	if err != nil {
		t.logger.Errorf("Couldn't create detached raft backend from request [%s]: %s", in.String(), err.Error())
	}
	return &pb.StartRaftResponse{
		RaftId:      in.RaftIdToUse,
		RaftAddress: t.myAddress,
	}, nil
}

func (t *copyCatTransport) StopRaft(ctx context.Context, in *pb.StopRaftRequest) (*pb.StopRaftResponse, error) {
	t.membership.stopRaft(in.RaftId)
	return &pb.StopRaftResponse{}, nil
}

// TODO: implement me
func (t *copyCatTransport) AddVoterToRaftGroup(ctx context.Context, in *pb.AddRaftRequest) (*pb.AddRaftResponse, error) {
	return &pb.AddRaftResponse{}, nil
}

func (t *copyCatTransport) AddLearnerToRaftGroup(ctx context.Context, in *pb.AddRaftRequest) (*pb.AddRaftResponse, error) {
	err := t.membership.addLearnerToRaftGroup(ctx, in.ExistingRaftId, in.NewRaftId)
	return &pb.AddRaftResponse{}, err
}

//////////////////////////////////////////
////////////////////////////////
// SECTION FOR THE RAFT TRANSPORT

func (t *copyCatTransport) Step(stream pb.RaftTransportService_StepServer) error {
	for { //ever...
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		err = t.membership.stepRaft(stream.Context(), *request.Message)
		if err != nil {
			err = fmt.Errorf("Invoking raft backend with id [%d] failed: %s", request.Message.To, err.Error())
			t.logger.Errorf("%s", err.Error())
			stream.Send(&pb.StepResp{Error: err.Error()})
		}

		stream.Send(&pb.StepResp{})
	}
}

func (t *copyCatTransport) sendMessages(msgs []raftpb.Message) *messageSendingResults {
	var results *messageSendingResults

	for _, msg := range msgs {
		client, err := t.membership.getRaftTransportServiceClientForRaftId(msg.To)
		if err != nil {
			if t.errorLogLimiter.Allow() {
				t.logger.Errorf("Can't create raft transport client: %s", err.Error())
			}
			results = t.addFailedMessage(results, msg)
			continue
		}

		// cancelling the context removes background go routines in the grpc implementation
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stream, err := client.Step(ctx)
		if err != nil {
			if t.errorLogLimiter.Allow() {
				t.logger.Errorf("Can't get stream: %s", err.Error())
			}
			continue
		}

		err = stream.Send(&pb.StepReq{Message: &msg})
		if err != nil {
			if t.errorLogLimiter.Allow() {
				t.logger.Errorf("Can't send on stream: %s", err.Error())
			}
			results = t.addFailedMessage(results, msg)
			continue
		} else if isMsgSnap(msg) {
			results = t.addSucceededSnapshotMessage(results, msg)
		}

		resp, err := stream.Recv()
		if err != nil {
			if t.errorLogLimiter.Allow() {
				t.logger.Errorf("Can't contact node for message %s: %s", msg.String(), err.Error())
			}
			results = t.addFailedMessage(results, msg)
			continue
		} else if resp.Error != "" {
			if t.errorLogLimiter.Allow() {
				t.logger.Errorf("Error: %s", resp.Error)
			}
			results = t.addFailedMessage(results, msg)
			continue
		}

		err = stream.CloseSend()
		if err != nil {
			if t.errorLogLimiter.Allow() {
				t.logger.Errorf("Can't close stream: %s", err.Error())
			}
		}
	}

	return results
}

func (t *copyCatTransport) addFailedMessage(results *messageSendingResults, msg raftpb.Message) *messageSendingResults {
	if results == nil {
		results = &messageSendingResults{}
	}

	results.failedMessages = append(results.failedMessages, msg)
	return results
}

func (t *copyCatTransport) addSucceededSnapshotMessage(results *messageSendingResults, msg raftpb.Message) *messageSendingResults {
	if results == nil {
		results = &messageSendingResults{}
	}

	results.succeededSnapshotMessages = append(results.succeededSnapshotMessages, msg)
	return results
}

type messageSendingResults struct {
	failedMessages            []raftpb.Message
	succeededSnapshotMessages []raftpb.Message
}

func (t *copyCatTransport) stop() {
	t.grpcServer.Stop()
}
