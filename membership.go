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
	"strconv"
	"strings"

	"github.com/hashicorp/serf/serf"
)

const (
	dbName = "serf.db"

	prefix = "__"

	serfMDKeySerfPort    = prefix + "serf_port"
	serfMDKeyCopyCatPort = prefix + "cat_port"
	serfMDKeyHost        = prefix + "host"
	serfMDKeyHostedItems = prefix + "items"
	serfMDKeyDataCenter  = prefix + "dc"
	serfMDKeyRack        = prefix + "rack"
)

// Membership uses serf under the covers to gossip metadata about the cluster.
// This includes the presence of nodes in the cluster, the raft groups in the cluster and their location,
// the data structures in the cluster and their location.
type membership struct {
	serfNodeID uint64
	serf       *serf.Serf
	crush      *crush
	logger     *log.Entry
}

func newMembership(config *Config) (*membership, error) {
	serfNodeID := randomRaftID()
	logger := config.logger.WithFields(log.Fields{
		"component":   "serf",
		"serf_node":   serfNodeID,
		"gossip_port": strconv.Itoa(config.GossipPort),
	})

	serfConfig := serf.DefaultConfig()
	serfEventCh := make(chan serf.Event, 64)
	serfConfig.EventCh = serfEventCh
	serfConfig.NodeName = uint64ToString(serfNodeID)
	serfConfig.EnableNameConflictResolution = true
	serfConfig.MemberlistConfig.BindAddr = config.Hostname
	serfConfig.MemberlistConfig.BindPort = config.GossipPort
	serfConfig.LogOutput = logger.WriterLevel(log.DebugLevel)
	if strings.HasSuffix(config.CopyCatDataDir, "/") {
		serfConfig.SnapshotPath = config.CopyCatDataDir + dbName
	} else {
		serfConfig.SnapshotPath = config.CopyCatDataDir + "/" + dbName
	}

	serfConfig.Tags = make(map[string]string)

	surf, err := serf.Create(serfConfig)
	if err != nil {
		return nil, err
	}

	if len(config.PeersToContact) > 0 {
		numContactedNodes, err := surf.Join(config.PeersToContact, true)
		if err != nil {
			logger.Errorf("Couldn't join serf cluster: %s", err.Error())
		}

		logger.Infof("Contacted %d out of %d nodes: %s", numContactedNodes, len(config.PeersToContact), strings.Join(config.PeersToContact, ","))
		if numContactedNodes == 0 {
			logger.Panicf("Cannot contact any of the defined nodes: [%s]", strings.Join(config.PeersToContact, ", "))
		}
	} else {
		logger.Info("No peers defined - starting a brandnew cluster!")
	}

	m := &membership{
		serfNodeID: serfNodeID,
		serf:       surf,
		crush:      newCrush(),
		logger:     logger,
	}

	go m.handleSerfEvents(serfEventCh)
	return m, nil
}

func (m *membership) handleSerfEvents(serfEventChannel <-chan serf.Event) {
	for {
		select {
		case serfEvent, ok := <-serfEventChannel:
			if !ok {
				return
			}
			//
			// Obviously we receive these events multiple times per actual event.
			// That means we need to do some sort of deduping.
			//
			switch serfEvent.EventType() {
			case serf.EventMemberJoin:
				m.handleMemberJoinEvent(serfEvent.(serf.MemberEvent))
			case serf.EventMemberUpdate:
				m.handleMemberUpdatedEvent(serfEvent.(serf.MemberEvent))
			case serf.EventMemberLeave:
				m.handleMemberLeaveEvent(serfEvent.(serf.MemberEvent))
			case serf.EventMemberFailed:
				m.handleMemberFailedEvent(serfEvent.(serf.MemberEvent))
			case serf.EventQuery:
				m.handleQuery((serfEvent).(*serf.Query))
			}
		case <-m.serf.ShutdownCh():
			m.logger.Error("Shutting down serf event loop")
			return
		}
	}
}

func (m *membership) handleMemberJoinEvent(me serf.MemberEvent) {}

func (m *membership) handleMemberUpdatedEvent(me serf.MemberEvent) {}

func (m *membership) handleMemberLeaveEvent(me serf.MemberEvent) {}

func (m *membership) handleMemberFailedEvent(me serf.MemberEvent) {}

func (m *membership) updateTags(key string, value string) error {
	t := m.serf.LocalMember().Tags
	t[key] = value
	return m.serf.SetTags(t)
}
