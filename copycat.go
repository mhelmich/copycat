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
	"strconv"
	"sync"
	"time"

	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type copyCatImpl struct {
	transport  *copyCatTransport
	membership copyCatMembership
	// this nodes CopyCay address in order to localize requests
	myAddress                     string
	addressToConnection           *sync.Map
	newCopyCatClientFunc          func(*sync.Map, string) (pb.CopyCatServiceClient, error)
	newInteractiveRaftBackendFunc func(config *Config, peers []pb.Peer) (*raftBackend, error)
	config                        *Config
	logger                        *log.Entry
}

func newCopyCat(config *Config) (*copyCatImpl, error) {
	m, err := newMembership(config)
	if err != nil {
		config.logger.Errorf("Can't create membership: %s", err.Error())
		return nil, err
	}

	t, err := newTransport(config, m)
	if err != nil {
		config.logger.Errorf("Can't create transport: %s", err.Error())
		return nil, err
	}

	config.raftTransport = t
	return &copyCatImpl{
		transport:                     t,
		membership:                    m,
		myAddress:                     config.hostname + ":" + strconv.Itoa(config.CopyCatPort),
		addressToConnection:           &sync.Map{},
		newCopyCatClientFunc:          _newCopyCatServiceClient,
		newInteractiveRaftBackendFunc: _newInteractiveRaftBackend,
		config: config,
		logger: config.logger,
	}, nil
}

func _newCopyCatServiceClient(m *sync.Map, address string) (pb.CopyCatServiceClient, error) {
	var conn *grpc.ClientConn
	var err error
	var val interface{}
	var ok bool
	var loaded bool

	val, ok = m.Load(address)
	if !ok {
		conn, err = grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}

		val, loaded = m.LoadOrStore(address, conn)
		if loaded {
			// somebody else stored the connection already
			// all of this was for nothing
			defer conn.Close()
		}
	}

	conn = val.(*grpc.ClientConn)
	return pb.NewCopyCatServiceClient(conn), nil
}

func _newInteractiveRaftBackend(config *Config, peers []pb.Peer) (*raftBackend, error) {
	return newInteractiveRaftBackend(config, peers)
}

// takes one operation, finds the leader remotely, and sends the operation to the leader
func (c *copyCatImpl) ConnectToDataStructure(id uint64, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer) {
	return nil, nil, nil, func() ([]byte, error) { return nil, nil }
}

// takes a data strucutre id, has this node join the raft group, finds the leader of the raft group, and tries to transfer leadership to this node
func (c *copyCatImpl) TakeOwnershipOfDataStructure(id uint64, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer) {
	return nil, nil, nil, func() ([]byte, error) { return nil, nil }
}

// takes a data structure id, joins the raft group as learner, assembles and exposes the raft log
func (c *copyCatImpl) SubscribeToDataStructure(id uint64, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer) {
	return nil, nil, nil, func() ([]byte, error) { return nil, nil }
}

func (c *copyCatImpl) startRaftGroup(dataStructureId uint64, numReplicas int) (*raftBackend, error) {
	// TODO: write glue code that lets you connect to a raft backend
	// or put more generically: figure out raft backend connectivity
	remoteRafts, err := c.choosePeersForNewDataStructure(dataStructureId, c.membership.getAllMetadata(), numReplicas)
	if err != nil {
		return nil, err
	}

	localRaft, err := c.createLocalRaft(dataStructureId, remoteRafts)
	if err != nil {
		return nil, err
	}

	return localRaft, nil
}

func (c *copyCatImpl) choosePeersForNewDataStructure(dataStructureId uint64, peersMetadata map[uint64]map[string]string, numPeers int) ([]pb.Peer, error) {
	if len(peersMetadata) <= 0 {
		return nil, fmt.Errorf("No peers to contact!")
	}

	newPeers := make([]pb.Peer, numPeers)
	idxOfPeerToContact := 0
	ch := make(chan *pb.Peer)

	for _, tags := range peersMetadata {
		if idxOfPeerToContact >= numPeers {
			break
		}

		localTags := tags
		go c.startRaftRemotely(ch, dataStructureId, localTags)
		idxOfPeerToContact++
	}

	timeout := time.Now().Add(5 * time.Second)
	j := 0

	for {
		select {
		case peer := <-ch:
			if peer == nil {

				// I got an empty response from one of the peers I contacted,
				// let me try another one...
				if idxOfPeerToContact >= len(peersMetadata) {
					return nil, fmt.Errorf("No more peers to contact")
				}

				count := 0
				for _, tags := range peersMetadata {
					if count == idxOfPeerToContact {
						go c.startRaftRemotely(ch, dataStructureId, tags)
						break
					}
					count++
				}

			} else {

				// if I got a valid response, put it in the array
				newPeers[j] = *peer
				j++
				if j >= numPeers {
					return newPeers, nil
				}

			}
		case <-time.After(time.Until(timeout)):
			// close all started rafts
			for _, p := range newPeers {
				go c.stopRaftRemotely(p)
			}
			return nil, fmt.Errorf("Couldn't find enough remote peers for raft creation and timed out")
		}
	}
}

func (c *copyCatImpl) startRaftRemotely(peerCh chan *pb.Peer, dataStructureId uint64, tags map[string]string) {
	addr := c.membership.getAddr(tags)
	if c.myAddress == addr {
		// signal to the listener that we need to retry another peer
		peerCh <- nil
		return
	}

	client, err := c.newCopyCatClientFunc(c.addressToConnection, addr)
	if err != nil {
		c.logger.Errorf("Can't connect to %s: %s", addr, err.Error())
		peerCh <- nil
		return
	}

	resp, err := client.StartRaft(context.TODO(), &pb.StartRaftRequest{DataStructureId: dataStructureId})
	if err != nil {
		c.logger.Errorf("Can't start a raft at %s: %s", addr, err.Error())
		// signal to the listener that we need to retry another peer
		peerCh <- nil
		return
	}

	c.logger.Infof("Started raft remotely on %s: %s", addr, resp.String())
	peerCh <- &pb.Peer{
		Id:          resp.RaftId,
		RaftAddress: resp.RaftAddress,
	}
}

func (c *copyCatImpl) stopRaftRemotely(peer pb.Peer) error {
	if peer.RaftAddress == "" {
		return nil
	}

	client, err := c.newCopyCatClientFunc(c.addressToConnection, peer.RaftAddress)
	if err != nil {
		return err
	}

	_, err = client.StopRaft(context.TODO(), &pb.StopRaftRequest{RaftId: peer.Id})
	return err
}

func (c *copyCatImpl) createLocalRaft(dataStructureId uint64, peers []pb.Peer) (*raftBackend, error) {
	backend, err := c.newInteractiveRaftBackendFunc(c.config, peers)
	if err != nil {
		return nil, err
	}

	c.membership.addDataStructureToRaftIdMapping(dataStructureId, backend.raftId)
	return backend, nil
}

func (c *copyCatImpl) Shutdown() {
	err := c.membership.stop()
	if err != nil {
		c.logger.Errorf("Error stopping membership: %s", err.Error())
	}

	c.addressToConnection.Range(func(key interface{}, value interface{}) bool {
		conn := value.(*grpc.ClientConn)
		defer conn.Close()
		return true
	})

	c.transport.stop()
}
