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
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
)

const (
	dbName = "serf.db"

	serfMDKeySerfPort    = "serf_port"
	serfMDKeyCopyCatPort = "cat_port"
	serfMDKeyHost        = "host"
	serfMDKeyHostedItems = "items"
	serfMDKeyLocation    = "loc"
)

// Membership uses serf under the covers to gossip metadata about the cluster.
// This includes the presence of nodes in the cluster, the raft groups in the cluster and their location,
// the data structures in the cluster and their location.
type membership struct {
	serf *serf.Serf

	// these maps cache the serf state locally
	// you can always fall back to querying the cluster if you want
	memberIdToTags           map[uint64]map[string]string
	raftIdToAddress          map[uint64]string
	dataStructureIdToRaftIds map[uint64]map[uint64]bool

	logger *log.Entry
}

func newMembership(config *CopyCatConfig) (*membership, error) {
	serfNodeId := uint64ToString(randomRaftId())
	logger := config.logger.WithFields(log.Fields{
		"component":   "serf",
		"serf_node":   serfNodeId,
		"gossip_port": strconv.Itoa(config.gossipPort),
	})

	serfConfig := serf.DefaultConfig()
	// it's important that this channel never blocks
	// if it blocks, the sender will block and therefore stop applying log entries
	// which means we're not up to date with the current cluster state anymore
	serfEventCh := make(chan serf.Event, 32)
	serfConfig.EventCh = serfEventCh
	serfConfig.NodeName = serfNodeId
	serfConfig.EnableNameConflictResolution = true
	serfConfig.MemberlistConfig.BindAddr = config.hostname
	serfConfig.MemberlistConfig.BindPort = config.gossipPort
	serfConfig.LogOutput = logger.WriterLevel(log.DebugLevel)
	if strings.HasSuffix(config.CopyCatDataDir, "/") {
		serfConfig.SnapshotPath = config.CopyCatDataDir + dbName
	} else {
		serfConfig.SnapshotPath = config.CopyCatDataDir + "/" + dbName
	}

	serfConfig.Tags = make(map[string]string)
	serfConfig.Tags[serfMDKeyHost] = config.hostname
	serfConfig.Tags[serfMDKeySerfPort] = strconv.Itoa(config.gossipPort)
	serfConfig.Tags[serfMDKeyCopyCatPort] = strconv.Itoa(config.CopyCatPort)
	serfConfig.Tags[serfMDKeyLocation] = config.Location
	serfConfig.Tags[serfMDKeyHostedItems] = proto.MarshalTextString(&pb.HostedItems{
		DataStructureToRaftMapping: make(map[uint64]uint64),
	})

	surf, err := serf.Create(serfConfig)
	if err != nil {
		return nil, err
	}

	if len(config.PeersToContact) > 0 {
		numContactedNodes, err := surf.Join(config.PeersToContact, true)
		if err != nil {
			logger.Errorf("Couldn't join serf cluster: %s", err.Error())
		}

		logger.Infof("Contacted %d out of %d nodes", numContactedNodes, len(config.PeersToContact))
	} else {
		logger.Info("No peers defined - starting a brandnew cluster!")
	}

	m := &membership{
		serf:                     surf,
		memberIdToTags:           make(map[uint64]map[string]string),
		raftIdToAddress:          make(map[uint64]string),
		dataStructureIdToRaftIds: make(map[uint64]map[uint64]bool),
		logger: logger,
	}

	go m.handleSerfEvents(serfEventCh)
	return m, nil
}

func (m *membership) handleSerfEvents(serfEventChannel <-chan serf.Event) {
	for { //ever...
		select {
		case serfEvent, ok := <-serfEventChannel:
			if !ok {
				return
			}

			m.logger.Infof("RECEIVING SERF EVENT: %s", serfEvent.String())

			//
			// Obviously we receive these events multiple times per actual event.
			// That means we need to do some sort of deduping.
			//
			switch serfEvent.EventType() {
			case serf.EventMemberJoin:
				m.handleMemberJoinEvent(serfEvent.(serf.MemberEvent))
			case serf.EventMemberUpdate:
				m.handleMemberUpdatedEvent(serfEvent.(serf.MemberEvent))
			case serf.EventMemberLeave, serf.EventMemberFailed:
				m.handleMemberLeaveEvent(serfEvent.(serf.MemberEvent))
			case serf.EventQuery:
				m.handleQuery((serfEvent).(*serf.Query))
			}
		case <-m.serf.ShutdownCh():
			return
		}
	}
}

func (m *membership) handleMemberJoinEvent(me serf.MemberEvent) {
	for _, mem := range me.Members {
		hostedItems := mem.Tags[serfMDKeyHostedItems]
		hi := &pb.HostedItems{}
		err := proto.UnmarshalText(hostedItems, hi)
		if err != nil {
			m.logger.Errorf("Can't unmarshall hosted items for node [%s]: %s", mem.Name, err.Error())
		}

		addr := m.getAddr(mem.Tags)
		memberId := stringToUint64(mem.Name)
		m.memberIdToTags[memberId] = mem.Tags

		for dsId, raftId := range hi.DataStructureToRaftMapping {
			m.raftIdToAddress[raftId] = addr
			raftIds, ok := m.dataStructureIdToRaftIds[dsId]
			if !ok {
				raftIds = make(map[uint64]bool)
			}
			raftIds[raftId] = false
			m.dataStructureIdToRaftIds[dsId] = raftIds
		}
	}
}

func (m *membership) handleMemberUpdatedEvent(me serf.MemberEvent) {
	m.handleMemberJoinEvent(me)
}

func (m *membership) handleMemberLeaveEvent(me serf.MemberEvent) {
	for _, mem := range me.Members {
		m.logger.Infof("Removing member: %s", mem.Name)
		// err :=proto.MarshalTextString(pb)
		// idStr := mem.Tags[serfMDKeyMemberId]
		// id := stringToUint64(idStr)
		// m.topology.delete(id)
	}
}

func (m *membership) handleQuery(query *serf.Query) {
	var err error
	i, err := strconv.Atoi(query.Name)
	if err != nil {
		m.logger.Errorf("Can't deserialize query type [%s]: %s", query.Name, err.Error())
	}

	m.logger.Infof("HANDLING QUERY: %s", query.String())

	var bites []byte
	switch pb.GossipQueryNames(i) {
	case pb.RaftIdQuery:
		bites, err = m.handleRaftIdQuery(query)
	case pb.DataStructureIdQuery:
		bites, err = m.handleDataStructureIdQuery(query)
	default:
		err = fmt.Errorf("I don't know query: %s", pb.GossipQueryNames(i).String())
	}

	if err != nil {
		m.logger.Errorf("Error processing query: %s", err.Error())
		return
	}

	err = query.Respond(bites)
	if err != nil {
		m.logger.Errorf("Error responding to query: %s", err.Error())
	}
}

func (m *membership) handleRaftIdQuery(query *serf.Query) ([]byte, error) {
	req := &pb.RaftIdQueryRequest{}
	err := req.Unmarshal(query.Payload)
	if err != nil {
		return nil, err
	}

	addr, ok := m.raftIdToAddress[req.RaftId]
	if !ok {
		return nil, fmt.Errorf("I don't know about raftId [%d] myself", req.RaftId)
	}

	resp := &pb.RaftIdQueryResponse{
		RaftId:  req.RaftId,
		Address: addr,
	}

	return resp.Marshal()
}

func (m *membership) handleDataStructureIdQuery(query *serf.Query) ([]byte, error) {
	m.logger.Infof("Handling data structure id query...")
	req := &pb.DataStructureIdRequest{}
	err := req.Unmarshal(query.Payload)
	if err != nil {
		return nil, err
	}

	addresses := m.getAddressesForDataStructureId(req.DataStructureId)
	if addresses == nil || len(addresses) == 0 {
		return nil, fmt.Errorf("I don't know about data structure id [%d] myself", req.DataStructureId)
	}

	resp := &pb.DataStructureIdResponse{
		Address: addresses[0],
	}

	m.logger.Infof("Sending response: %s", resp.String())
	return resp.Marshal()
}

func (m *membership) addDsToRaftIdMapping(dsId uint64, raftId uint64) error {
	tags := m.serf.LocalMember().Tags
	hostedItems := tags[serfMDKeyHostedItems]
	hi := &pb.HostedItems{}
	err := proto.UnmarshalText(hostedItems, hi)
	if err != nil {
		m.logger.Errorf("Can't unmarshall hosted items: %s", err.Error())
		return err
	}

	// protobuf will optimize empty maps away and provoke a seg fault here
	if hi.DataStructureToRaftMapping == nil {
		hi.DataStructureToRaftMapping = make(map[uint64]uint64)
	}

	hi.DataStructureToRaftMapping[dsId] = raftId

	setMe := make(map[string]string)
	setMe[serfMDKeyHostedItems] = proto.MarshalTextString(hi)
	// this will update the nodes metadata and broadcast it out
	// blocks until broadcasting was successful or timed out
	// this update will be processed via the regular membership event processing
	return m.serf.SetTags(setMe)
}

func (m *membership) findDataStructureWithId(id uint64) (string, error) {
	req := &pb.DataStructureIdRequest{DataStructureId: id}
	data, err := req.Marshal()
	if err != nil {
		return "", err
	}

	serfQueryResp, err := m.serf.Query(strconv.Itoa(int(pb.DataStructureIdQuery)), data, &serf.QueryParam{})
	if err != nil {
		return "", err
	}

	defer serfQueryResp.Close()
	serfRespCh := serfQueryResp.ResponseCh()
	m.logger.Infof("Sent serf query: %s", req.String())

	for {
		select {
		case serfResp, ok := <-serfRespCh:
			if !ok {
				return "", fmt.Errorf("Serf response channel was closed")
			}

			m.logger.Infof("Got response %v", serfResp)
			if serfResp.Payload != nil {
				resp := &pb.DataStructureIdResponse{}
				err = resp.Unmarshal(serfResp.Payload)
				if err == nil {
					return resp.Address, nil
				}
			}
		case <-time.After(time.Until(serfQueryResp.Deadline())):
			return "", fmt.Errorf("Serf query timed out: %s", req.String())
		}
	}
}

func (m *membership) findPeersForNewDataStructure() {
}

func (m *membership) getAddressesForDataStructureId(dsId uint64) []string {
	raftIdsMap, ok := m.dataStructureIdToRaftIds[dsId]
	if !ok {
		m.logger.Infof("I don't know data structure with id [%d]", dsId)
		return make([]string, 0)
	}

	addrs := make([]string, len(raftIdsMap))
	i := 0
	for raftId := range raftIdsMap {
		addr, ok := m.raftIdToAddress[raftId]
		if ok {
			addrs[i] = addr
			i++
		}
	}
	return addrs[:i]
}

func (m *membership) getAddressForRaftId(raftId uint64) string {
	return m.raftIdToAddress[raftId]
}

func (m *membership) getAddr(tags map[string]string) string {
	return tags[serfMDKeyHost] + ":" + tags[serfMDKeyCopyCatPort]
}

func (m *membership) stop() error {
	err := m.serf.Leave()
	if err != nil {
		m.logger.Errorf("Error leaving serf cluster: %s", err.Error())
	}
	return m.serf.Shutdown()
}
