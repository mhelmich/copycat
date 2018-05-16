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

func newCopyCat(config *CopyCatConfig) (*copyCatImpl, error) {
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
	}, nil
}

type copyCatImpl struct {
	transport  *copyCatTransport
	membership *membership
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
func (c *copyCatImpl) FollowDataStructure(id uint64, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer) {
	return nil, nil, nil, func() ([]byte, error) { return nil, nil }
}

func (c *copyCatImpl) Shutdown() {
	err := c.membership.stop()
	if err != nil {
		c.logger.Errorf("Error stopping membership: %s", err.Error())
	}
	c.transport.stop()
}
