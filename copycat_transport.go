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
	"strconv"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type copyCatTransport struct {
	config             *Config
	grpcServer         *grpc.Server
	myAddress          string
	raftBackends       map[uint64]transportRaftBackend
	membership         transportMembership
	newRaftBackendFunc func(uint64, *Config) (transportRaftBackend, error) // pulled out for testing
	logger             *log.Entry
}

func newTransport(config *Config, membership transportMembership) (*copyCatTransport, error) {
	logger := config.logger.WithFields(log.Fields{
		"component": "copycat_transport",
	})

	myAddress := fmt.Sprintf("%s:%d", config.hostname, config.CopyCatPort)
	lis, err := net.Listen("tcp", myAddress)
	if err != nil {
		logger.Fatalf("failed to listen: %v", err)
		return nil, err
	}

	transport := &copyCatTransport{
		config:             config,
		grpcServer:         grpc.NewServer(),
		myAddress:          myAddress,
		raftBackends:       make(map[uint64]transportRaftBackend),
		membership:         membership,
		newRaftBackendFunc: _transportNewRaftBackend,
		logger:             logger,
	}

	pb.RegisterCopyCatServiceServer(transport.grpcServer, transport)
	pb.RegisterRaftTransportServiceServer(transport.grpcServer, transport)
	go transport.grpcServer.Serve(lis)

	return transport, nil
}

// Yet another level of indirection so that this method can return an interface
// used for unit testing
func _transportNewRaftBackend(newRaftId uint64, config *Config) (transportRaftBackend, error) {
	return newDetachedRaftBackendWithId(newRaftId, config)
}

//////////////////////////////////////////
////////////////////////////////
// SECTION FOR THE COPYCAT SERVICE

func (t *copyCatTransport) StartRaft(ctx context.Context, in *pb.StartRaftRequest) (*pb.StartRaftResponse, error) {
	newRaftId := randomRaftId()
	// A raft backend is started in join mode but without specifying other peers.
	// It will just sit there and do nothing until a leader with higher term contacts it.
	// After that the new backend will try to respond to the messages it has been receiving and join the cluster.
	backend, err := t.newRaftBackendFunc(newRaftId, t.config)
	if err != nil {
		t.logger.Errorf("Can't create raft backend: %s", err.Error())
		return nil, err
	}

	t.raftBackends[newRaftId] = backend
	t.membership.addDataStructureToRaftIdMapping(in.DataStructureId, newRaftId)
	return &pb.StartRaftResponse{
		RaftId:      newRaftId,
		RaftAddress: t.config.hostname + ":" + strconv.Itoa(t.config.CopyCatPort),
	}, nil
}

func (t *copyCatTransport) StopRaft(ctx context.Context, in *pb.StopRaftRequest) (*pb.StopRaftResponse, error) {
	rb, ok := t.raftBackends[in.RaftId]
	if ok {
		delete(t.raftBackends, in.RaftId)
		defer rb.stop()
	}
	return &pb.StopRaftResponse{}, nil
}

//////////////////////////////////////////
////////////////////////////////
// SECTION FOR THE RAFT TRANSPORT

func (t *copyCatTransport) Send(stream pb.RaftTransportService_SendServer) error {
	for { //ever...
		request, err := stream.Recv()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}

		backend, ok := t.raftBackends[request.Message.To]
		if ok {
			// invoke the raft state machine
			err := backend.step(stream.Context(), *request.Message)
			if err == nil {
				stream.Send(&pb.SendResp{Error: pb.NoError})
			} else {
				t.logger.Errorf("Invoking raft backend with id [%d] failed: %s", request.Message.To, err.Error())
				stream.Send(&pb.SendResp{Error: pb.NoError})
			}
		} else {
			t.logger.Errorf("Can't find processor for raft id %d", request.Message.To)
		}
	}
}

func (t *copyCatTransport) sendMessages(msgs []raftpb.Message) {
	for _, msg := range msgs {
		addr := t.membership.getAddressForRaftId(msg.To)
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			t.logger.Errorf("Can't create connection: %s", err.Error())
		}

		defer conn.Close()
		client := pb.NewRaftTransportServiceClient(conn)
		stream, err := client.Send(context.Background())
		if err != nil {
			t.logger.Errorf("Can't get stream: %s", err.Error())
		}

		err = stream.Send(&pb.SendReq{Message: &msg})
		if err != nil {
			t.logger.Errorf("Can't send on stream: %s", err.Error())
		}

		resp, err := stream.Recv()
		if err != nil {
			t.logger.Errorf("Can't contact node for message %s: %s", msg.String(), err.Error())
		} else if resp.Error != pb.NoError {
			t.logger.Errorf("Error: %s", resp.Error.String())
		}

		defer stream.CloseSend()
	}
}

//////////////////////////////////////////
////////////////////////////////
// SECTION FOR UTILS

func (t *copyCatTransport) doIHostDataStructure(dataStructureId uint64) (bool, string) {
	addresses := t.membership.getAddressesForDataStructureId(dataStructureId)
	if len(addresses) == 0 {
		return false, ""
	}

	for _, addr := range addresses {
		if addr == t.myAddress {
			return true, ""
		}
	}

	// yeah, I'm not hosting that
	// just return a random one for now
	return false, addresses[0]
}

func (t *copyCatTransport) stop() {
	// TODO: stop all local raft backends
	t.grpcServer.Stop()
}
