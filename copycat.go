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
	"bytes"
	"fmt"
	"os"
	"strconv"

	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
)

type copyCatImpl struct {
	transport  *copyCatTransport
	membership membershipProxy
	config     *Config
	logger     *log.Entry
}

func sanitizeConfig(config *Config) {
	if config.logger == nil {
		config.logger = log.WithFields(log.Fields{
			"component":    "copycat",
			"host":         config.Hostname,
			"copycat_port": strconv.Itoa(config.CopyCatPort),
			"serf_port":    strconv.Itoa(config.GossipPort),
		})
	}

	if config.CopyCatDataDir == "" {
		config.logger.Panic("CopyCatDataDir is empty.")
	}

	if _, err := os.Stat(config.CopyCatDataDir); os.IsNotExist(err) {
		err := os.MkdirAll(config.CopyCatDataDir, os.ModePerm)
		if err != nil {
			config.logger.Panicf("Can't create dir: %s", config.CopyCatDataDir)
		}
	}
}

func newCopyCat(config *Config) (*copyCatImpl, error) {
	sanitizeConfig(config)

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
	cc := &copyCatImpl{
		transport:  t,
		membership: cache,
		config:     config,
		logger:     config.logger,
	}

	cc.logger.Infof("Started CopyCat instance with config: %s", config.String())
	return cc, nil
}

func (c *copyCatImpl) NewDataStructureID() (*ID, error) {
	return newId()
}

func (c *copyCatImpl) AllocateNewDataStructure(opts ...AllocationOption) (*ID, error) {
	var peer *pb.RaftPeer
	var err error
	var id *ID

	// TODO - limit the number of iterations
	// as long as it takes, try making a new unique id that
	// doesn't exist in the cluster yet (aka is really unique)
	for peer == nil || err != nil {
		id, err = newId()
		if err != nil {
			continue
		}

		peer, err = c.membership.onePeerForDataStructureId(id)
	}

	// apply all provided allocation options
	allocOpts := defaultAllocationOptions()
	for _, opt := range opts {
		opt(allocOpts)
	}

	// fire up a raft group
	remoteRaftPeers, err := c.membership.startNewRaftGroup(id, allocOpts.dataCenterReplicas)
	if err != nil {
		c.logger.Errorf("Can't connect to data structure [%s]: %s", id, err.Error())
		return nil, err
	}

	// print out ids of started rafts
	buffer := bytes.Buffer{}
	for _, p := range remoteRaftPeers {
		buffer.WriteString(fmt.Sprintf(" %d %x ", p.RaftId, p.RaftId))
	}
	c.logger.Infof("Allocated new raft group [%s] for data structure [%s]", buffer.String(), id.String())

	return id, nil
}

func (c *copyCatImpl) SubscribeToDataStructureWithStringID(id string, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer, error) {
	i, err := parseIdFromString(id)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return c.SubscribeToDataStructure(i, provider)
}

// takes a data structure id, joins the raft group as learner, assembles and exposes the raft log
func (c *copyCatImpl) SubscribeToDataStructure(id *ID, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer, error) {
	var interactiveBackend *raftBackend
	var err error
	var remoteRaftPeers []*pb.RaftPeer
	var remoteRaftPeer *pb.RaftPeer

	remoteRaftPeer, _ = c.membership.onePeerForDataStructureId(id)
	if remoteRaftPeer == nil {
		c.logger.Infof("Can't find data structure [%s] anywhere. Starting new raft group...", id)
		// TODO: revisit the concept of number of replicas
		// In my mind the number of replicas is just a proxy for how paranoid you are...
		// Maybe it's easier for the user to fhink about it as different use cases.
		// Meh, paranoia and consistency vs real consistency
		remoteRaftPeers, err = c.membership.startNewRaftGroup(id, 2)
		if err != nil {
			c.logger.Errorf("Can't connect to data structure [%s]: %s", id, err.Error())
			return nil, nil, nil, nil, err
		}

		interactiveBackend, err = c.subscribeToExistingRaftGroup(id, c.config, remoteRaftPeers[0], provider)
		if err != nil {
			c.logger.Errorf("Can't connect to data structure [%s]: %s", id, err.Error())
			return nil, nil, nil, nil, err
		}
	} else {
		c.logger.Infof("Found data structure [%s] on peer: %s", id, remoteRaftPeer.String())
		interactiveBackend, err = c.subscribeToExistingRaftGroup(id, c.config, remoteRaftPeer, provider)
		if err != nil {
			c.logger.Errorf("Can't connect to data structure [%s]: %s", id, err.Error())
			return nil, nil, nil, nil, err
		}
	}

	return interactiveBackend.proposeChan, interactiveBackend.commitChan, interactiveBackend.errorChan, func() ([]byte, error) { return interactiveBackend.snapshot() }, nil
}

func (c *copyCatImpl) subscribeToExistingRaftGroup(dataStructureId *ID, config *Config, peer *pb.RaftPeer, provider SnapshotProvider) (*raftBackend, error) {
	// 1. create new interactive raft in join mode (without any peers)
	backend, err := c.membership.newInteractiveRaftBackendForExistingGroup(dataStructureId, config, provider)
	if err != nil {
		return nil, err
	}
	// 2. try to contact peers and add the newly created raft to their raft group
	err = c.membership.addRaftToGroupRemotely(backend.raftId, peer)
	// 3. -> profit
	return backend, err
}

func (c *copyCatImpl) Shutdown() {
	c.logger.Warn("Shutting down CopyCat!")
	err := c.membership.stop()
	if err != nil {
		c.logger.Errorf("Error stopping membership: %s", err.Error())
	}

	c.transport.stop()
}
