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
	"time"

	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

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
		transport:  t,
		membership: m,
		myAddress:  config.hostname + ":" + strconv.Itoa(config.CopyCatPort),
		config:     config,
		logger:     config.logger,
	}, nil
}

type copyCatImpl struct {
	transport  *copyCatTransport
	membership *membership
	myAddress  string
	config     *Config
	logger     *log.Entry
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

func (c *copyCatImpl) startRaftGroup(dataStructureId uint64) error {
	// TODO: write glue code that lets you connect to a raft backend
	// or put more generically: figure out raft backend connectivity
	// remoteRafts, err := c.choosePeersForNewDataStructure(dataStructureId, c.membership.getAllMetadata(), 2)
	// if err != nil {
	// 	return err
	// }
	//
	// localRaft, err := c.createLocalRaft(dataStructureId)
	// if err != nil {
	// 	return err
	// }

	return nil
}

func (c *copyCatImpl) choosePeersForNewDataStructure(dataStructureId uint64, peersMetadata map[uint64]map[string]string, numPeers int) ([]pb.Peer, error) {
	newPeers := make([]pb.Peer, numPeers)
	idxOfPeerToContact := 0
	ch := make(chan *pb.Peer)

	for _, tags := range peersMetadata {
		if idxOfPeerToContact >= numPeers {
			break
		}

		go c.startRaftRemotely(ch, dataStructureId, tags)
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

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		c.logger.Errorf("Can't connect to %s: %s", addr, err.Error())
		// signal to the listener that we need to retry another peer
		peerCh <- nil
		return
	}

	defer conn.Close()
	client := pb.NewCopyCatServiceClient(conn)
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

func (c *copyCatImpl) createLocalRaft(dataStructureId uint64) (*raftBackend, error) {
	backend, err := newInteractiveRaftBackend(c.config)
	if err != nil {
		return nil, err
	}

	c.membership.addDsToRaftIdMapping(dataStructureId, backend.raftId)
	return backend, nil
}

func (c *copyCatImpl) Shutdown() {
	err := c.membership.stop()
	if err != nil {
		c.logger.Errorf("Error stopping membership: %s", err.Error())
	}
	c.transport.stop()
}
