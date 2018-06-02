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
	log "github.com/sirupsen/logrus"
)

type copyCatImpl struct {
	transport  *copyCatTransport
	membership membershipProxy
	// this nodes CopyCay address in order to localize requests
	myAddress string
	config    *Config
	logger    *log.Entry
}

func newCopyCat(config *Config) (*copyCatImpl, error) {
	cache, err := newMembershipCache(config)
	if err != nil {
		config.logger.Errorf("Can't create membership: %s", err.Error())
		return nil, err
	}

	t, err := newTransport(config, cache)
	if err != nil {
		config.logger.Errorf("Can't create transport: %s", err.Error())
		return nil, err
	}

	// HACK
	// this will be used in raft backends
	config.raftTransport = t
	return &copyCatImpl{
		transport:  t,
		membership: cache,
		myAddress:  config.address(),
		config:     config,
		logger:     config.logger,
	}, nil
}

// takes one operation, finds the leader remotely, and sends the operation to the leader
func (c *copyCatImpl) ConnectToDataStructure(id uint64, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer, error) {
	var interactiveBackend *raftBackend
	var err error

	peers := c.membership.peersForDataStructureId(id)
	if len(peers) > 0 {
		interactiveBackend, err = c.membership.newInteractiveRaftBackend(id, c.config, peers, provider)
	} else {
		interactiveBackend, err = c.startNewRaftGroup(id, 1, provider)
	}

	if err != nil {
		c.logger.Errorf("Can't connect to data structure: %s", err.Error())
		return nil, nil, nil, nil, err
	}
	return interactiveBackend.proposeChan, interactiveBackend.commitChan, interactiveBackend.errorChan, func() ([]byte, error) { return interactiveBackend.snapshot() }, nil
}

// takes a data strucutre id, has this node join the raft group, finds the leader of the raft group, and tries to transfer leadership to this node
func (c *copyCatImpl) TakeOwnershipOfDataStructure(id uint64, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer) {
	return nil, nil, nil, func() ([]byte, error) { return nil, nil }
}

// takes a data structure id, joins the raft group as learner, assembles and exposes the raft log
func (c *copyCatImpl) SubscribeToDataStructure(id uint64, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer) {
	return nil, nil, nil, func() ([]byte, error) { return nil, nil }
}

func (c *copyCatImpl) startNewRaftGroup(dataStructureId uint64, numReplicas int, provider SnapshotProvider) (*raftBackend, error) {
	c.logger.Infof("Starting a new raft group around data structure with id [%d] and [%d] replicas", dataStructureId, numReplicas)
	remoteRafts, err := c.membership.chooseReplicaNode(dataStructureId, numReplicas)
	if err != nil {
		return nil, err
	}

	localRaft, err := c.membership.newInteractiveRaftBackend(dataStructureId, c.config, remoteRafts, provider)
	if err != nil {
		return nil, err
	}

	return localRaft, nil
}

func (c *copyCatImpl) Shutdown() {
	c.logger.Warn("Shutting down CopyCat!")
	err := c.membership.stop()
	if err != nil {
		c.logger.Errorf("Error stopping membership: %s", err.Error())
	}

	c.transport.stop()
}
