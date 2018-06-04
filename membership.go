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
	"sync"
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
	serfNodeId uint64
	serf       *serf.Serf
	// this mutex protects concurrent access to serf tags
	// as you can see this is not exactly a fine-grained approach
	// if this becomes a problem, we can think about a better way to do this
	serfTagMutex *sync.Mutex

	// these maps cache the serf state locally
	// you can always fall back to querying the cluster if you want
	// uint64 - the id of the serf node
	// map[string]string - all tags of a particular serf node
	memberIdToTags           *sync.Map
	raftIdToAddress          map[uint64]string
	dataStructureIdToRaftIds map[uint64]map[uint64]bool

	logger *log.Entry
}

func newMembership(config *Config) (*membership, error) {
	serfNodeId := randomRaftId()
	logger := config.logger.WithFields(log.Fields{
		"component":   "serf",
		"serf_node":   serfNodeId,
		"gossip_port": strconv.Itoa(config.GossipPort),
	})

	serfConfig := serf.DefaultConfig()
	// it's important that this channel never blocks
	// if it blocks, the sender will block and therefore stop applying log entries
	// which means we're not up to date with the current cluster state anymore
	serfEventCh := make(chan serf.Event, 32)
	serfConfig.EventCh = serfEventCh
	serfConfig.NodeName = uint64ToString(serfNodeId)
	serfConfig.EnableNameConflictResolution = true
	serfConfig.MemberlistConfig.BindAddr = config.hostname
	serfConfig.MemberlistConfig.BindPort = config.GossipPort
	serfConfig.LogOutput = logger.WriterLevel(log.DebugLevel)
	if strings.HasSuffix(config.CopyCatDataDir, "/") {
		serfConfig.SnapshotPath = config.CopyCatDataDir + dbName
	} else {
		serfConfig.SnapshotPath = config.CopyCatDataDir + "/" + dbName
	}

	serfConfig.Tags = make(map[string]string)
	serfConfig.Tags[serfMDKeyHost] = config.hostname
	serfConfig.Tags[serfMDKeySerfPort] = strconv.Itoa(config.GossipPort)
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

		logger.Infof("Contacted %d out of %d nodes: %s", numContactedNodes, len(config.PeersToContact), strings.Join(config.PeersToContact, ","))
	} else {
		logger.Info("No peers defined - starting a brandnew cluster!")
	}

	m := &membership{
		serfNodeId:               serfNodeId,
		serf:                     surf,
		serfTagMutex:             &sync.Mutex{},
		memberIdToTags:           &sync.Map{},
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
			m.logger.Error("Shutting down serf event loop")
			return
		}
	}
}

func (m *membership) handleMemberJoinEvent(me serf.MemberEvent) {
	m.serfTagMutex.Lock()
	defer m.serfTagMutex.Unlock()

	for _, mem := range me.Members {
		hostedItems := mem.Tags[serfMDKeyHostedItems]
		hi := &pb.HostedItems{}
		err := proto.UnmarshalText(hostedItems, hi)
		if err != nil {
			m.logger.Errorf("Can't unmarshall hosted items for node [%s]: %s", mem.Name, err.Error())
		}

		addr := m.getAddr(mem.Tags)
		memberId := stringToUint64(mem.Name)
		m.memberIdToTags.Store(memberId, mem.Tags)

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
	m.serfTagMutex.Lock()
	defer m.serfTagMutex.Unlock()

	for _, mem := range me.Members {
		hostedItems := mem.Tags[serfMDKeyHostedItems]
		hi := &pb.HostedItems{}
		err := proto.UnmarshalText(hostedItems, hi)
		if err != nil {
			m.logger.Errorf("Can't unmarshall hosted items for node [%s]: %s", mem.Name, err.Error())
		}

		memberId := stringToUint64(mem.Name)
		m.memberIdToTags.Delete(memberId)

		for dsId, raftId := range hi.DataStructureToRaftMapping {
			delete(m.raftIdToAddress, raftId)
			m.logger.Infof("Deleted %d", raftId)
			delete(m.dataStructureIdToRaftIds[dsId], raftId)
		}
	}
}

func (m *membership) handleQuery(query *serf.Query) {
	var err error
	i, err := strconv.Atoi(query.Name)
	if err != nil {
		m.logger.Errorf("Can't deserialize query type [%s]: %s", query.Name, err.Error())
	}

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

	peers := m.peersForDataStructureId(req.DataStructureId)
	if peers == nil || len(peers) == 0 {
		return nil, fmt.Errorf("I don't know about data structure id [%d] myself", req.DataStructureId)
	}

	resp := &pb.DataStructureIdResponse{
		RaftId:  peers[0].Id,
		Address: peers[0].RaftAddress,
	}

	m.logger.Infof("Sending response: %s", resp.String())
	return resp.Marshal()
}

func (m *membership) addDataStructureToRaftIdMapping(dataStructureId uint64, raftId uint64) error {
	m.serfTagMutex.Lock()
	defer m.serfTagMutex.Unlock()

	hi, err := m.getMyHostedItems()
	if err != nil {
		return err
	}

	// protobuf will optimize empty maps away and provoke a seg fault here
	if hi.DataStructureToRaftMapping == nil {
		hi.DataStructureToRaftMapping = make(map[uint64]uint64)
	}

	hi.DataStructureToRaftMapping[dataStructureId] = raftId

	// this will update the nodes metadata and broadcast it out
	// blocks until broadcasting was successful or timed out
	// this update will be processed via the regular membership event processing
	return m.updateTags(serfMDKeyHostedItems, proto.MarshalTextString(hi))
}

func (m *membership) removeDataStructureToRaftIdMapping(raftId uint64) error {
	m.serfTagMutex.Lock()
	defer m.serfTagMutex.Unlock()

	hi, err := m.getMyHostedItems()
	if err != nil {
		return err
	}

	if hi.DataStructureToRaftMapping == nil {
		return nil
	}

	var dataStructureIdToDelete uint64
	for dsId, rId := range hi.DataStructureToRaftMapping {
		if rId == raftId {
			dataStructureIdToDelete = dsId
			break
		}
	}
	delete(hi.DataStructureToRaftMapping, dataStructureIdToDelete)

	// this will update the nodes metadata and broadcast it out
	// blocks until broadcasting was successful or timed out
	// this update will be processed via the regular membership event processing
	return m.updateTags(serfMDKeyHostedItems, proto.MarshalTextString(hi))
}

func (m *membership) updateTags(key string, value string) error {
	t := m.serf.LocalMember().Tags
	t[key] = value
	return m.serf.SetTags(t)
}

func (m *membership) getMyHostedItems() (*pb.HostedItems, error) {
	tags := m.serf.LocalMember().Tags
	hostedItems := tags[serfMDKeyHostedItems]
	hi := &pb.HostedItems{}
	err := proto.UnmarshalText(hostedItems, hi)
	if err != nil {
		err = fmt.Errorf("Can't unmarshall hosted items: %s", err.Error())
		m.logger.Errorf("%s", err.Error())
		return nil, err
	}

	return hi, nil
}

func (m *membership) findDataStructureWithId(id uint64) (*pb.Peer, error) {
	req := &pb.DataStructureIdRequest{DataStructureId: id}
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}

	serfQueryResp, err := m.serf.Query(strconv.Itoa(int(pb.DataStructureIdQuery)), data, m.serf.DefaultQueryParams())
	if err != nil {
		return nil, err
	}

	defer serfQueryResp.Close()
	serfRespCh := serfQueryResp.ResponseCh()
	m.logger.Infof("Sent serf query: %s", req.String())

	for {
		select {
		case serfResp, ok := <-serfRespCh:
			if !ok {
				return nil, fmt.Errorf("Serf response channel was closed")
			}

			if serfResp.Payload != nil {
				resp := &pb.DataStructureIdResponse{}
				err = resp.Unmarshal(serfResp.Payload)
				if err == nil {
					return &pb.Peer{
						Id:          resp.RaftId,
						RaftAddress: resp.Address,
					}, nil
				}
			}
		case <-time.After(time.Until(serfQueryResp.Deadline())):
			return nil, fmt.Errorf("Serf query timed out: %s", req.String())
		}
	}
}

func (m *membership) getAddressForRaftId(raftId uint64) string {
	m.serfTagMutex.Lock()
	defer m.serfTagMutex.Unlock()

	return m.raftIdToAddress[raftId]
}

func (m *membership) getAddr(tags map[string]string) string {
	return tags[serfMDKeyHost] + ":" + tags[serfMDKeyCopyCatPort]
}

func (m *membership) getAddressForPeer(peerId uint64) string {
	val, ok := m.memberIdToTags.Load(peerId)
	if ok {
		tags := val.(map[string]string)
		return m.getAddr(tags)
	}

	return ""
}

// For connection use cases, we just need one peer in the raft group
// We don't care to get the complete list of peers - one is enough.
// What we do care about though is that if we say the data structure doesn't exist,
// it really doesn't exist.
func (m *membership) onePeerForDataStructureId(dataStructureId uint64) (*pb.Peer, error) {
	m.serfTagMutex.Lock()
	defer m.serfTagMutex.Unlock()

	raftIdsMap, ok := m.dataStructureIdToRaftIds[dataStructureId]
	// we got nothing in our local cache
	// let's make sure the entire cluster doesn't know about this data structure
	if !ok {
		peer, err := m.findDataStructureWithId(dataStructureId)
		if err != nil {
			return nil, err
		} else if peer == nil {
			return nil, fmt.Errorf("Can't find data structure with id [%d]", dataStructureId)
		}

		return peer, nil
	}

	// cache hit!
	for raftId := range raftIdsMap {
		v, ok := m.raftIdToAddress[raftId]
		if ok {
			return &pb.Peer{
				Id:          raftId,
				RaftAddress: v,
			}, nil
		}
	}

	return nil, fmt.Errorf("Can't find data structure with id [%d]", dataStructureId)
}

func (m *membership) peersForDataStructureId(dataStructureId uint64) []pb.Peer {
	raftIdsMap, ok := m.dataStructureIdToRaftIds[dataStructureId]
	if !ok {
		m.logger.Infof("I don't know data structure with id [%d]", dataStructureId)
		return make([]pb.Peer, 0)
	}

	peers := make([]pb.Peer, len(raftIdsMap))
	i := 0
	for raftId := range raftIdsMap {
		v, ok := m.raftIdToAddress[raftId]
		if ok {
			peers[i] = pb.Peer{
				Id:          raftId,
				RaftAddress: v,
			}
			i++
		}
	}
	return peers[:i]
}

func (m *membership) myGossipNodeId() uint64 {
	return m.serfNodeId
}

func (m *membership) pickFromMetadata(picker func(peerId uint64, tags map[string]string) bool, numItemsToPick int, avoidMe []uint64) []uint64 {
	pickedPeers := make([]uint64, numItemsToPick)
	idx := 0

	// cache in map for fast contains
	misfits := make(map[uint64]bool)
	for _, v := range avoidMe {
		misfits[v] = false
	}

	m.memberIdToTags.Range(func(key interface{}, value interface{}) bool {
		id := key.(uint64)
		t := value.(map[string]string)
		// if it's in the list of nodes to avoid, there's no point in running the picker
		_, contains := misfits[id]
		if !contains {
			shouldPick := picker(id, t)
			// add the peer if the picker returned true
			if shouldPick {
				pickedPeers[idx] = id
				idx++
			}
		}

		// stop iterating if the array is full
		return idx < numItemsToPick
	})

	return pickedPeers[:idx]
}

func (m *membership) stop() error {
	m.logger.Warn("Shutting down serf!")
	err := m.serf.Leave()
	if err != nil {
		m.logger.Errorf("Error leaving serf cluster: %s", err.Error())
	}
	return m.serf.Shutdown()
}
