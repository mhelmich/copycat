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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/time/rate"
)

const (
	dbName = "serf.db"

	serfMDKeySerfPort    = "serf_port"
	serfMDKeyCopyCatPort = "cat_port"
	serfMDKeyHost        = "host"
	serfMDKeyHostedItems = "items"
	serfMDKeyDataCenter  = "dc"
	serfMDKeyRack        = "rack"
)

// Membership uses serf under the covers to gossip metadata about the cluster.
// This includes the presence of nodes in the cluster, the raft groups in the cluster and their location,
// the data structures in the cluster and their location.
type membership struct {
	serfNodeId uint64
	serf       *serf.Serf
	// TODO - naive implementation
	// this mutex protects concurrent access to serf tags
	// as you can see this is not exactly a fine-grained approach
	// if this becomes a problem, we can think about a better way to do this
	serfTagMutex *sync.Mutex

	// these maps cache the serf state locally
	// you can always fall back to querying the cluster if you want
	// uint64 - the id of the serf node
	// map[string]string - all tags of a particular serf node
	memberIdToTags            *sync.Map
	raftIdToAddress           map[uint64]string
	dataStructureIdToRaftIds  map[ID]map[uint64]bool
	dataStructureIdToMetadata map[ID]*pb.DataStructureMetadata
	crush                     *crush

	// this sync map mimics the following map: map[uint64]*rate.Limiter
	serfQueryRaftLimiers *sync.Map
	logger               *log.Entry

	rehashDataStructureIdChan chan *ID
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
	serfConfig.MemberlistConfig.BindAddr = config.Hostname
	serfConfig.MemberlistConfig.BindPort = config.GossipPort
	serfConfig.LogOutput = logger.WriterLevel(log.DebugLevel)
	if strings.HasSuffix(config.CopyCatDataDir, "/") {
		serfConfig.SnapshotPath = config.CopyCatDataDir + dbName
	} else {
		serfConfig.SnapshotPath = config.CopyCatDataDir + "/" + dbName
	}

	serfConfig.Tags = make(map[string]string)
	serfConfig.Tags[serfMDKeyHost] = config.Hostname
	serfConfig.Tags[serfMDKeySerfPort] = strconv.Itoa(config.GossipPort)
	serfConfig.Tags[serfMDKeyCopyCatPort] = strconv.Itoa(config.CopyCatPort)
	serfConfig.Tags[serfMDKeyDataCenter] = config.DataCenter
	serfConfig.Tags[serfMDKeyRack] = config.Rack
	serfConfig.Tags[serfMDKeyHostedItems] = proto.MarshalTextString(&pb.HostedItems{})

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
		serfNodeId:                serfNodeId,
		serf:                      surf,
		serfTagMutex:              &sync.Mutex{},
		memberIdToTags:            &sync.Map{},
		raftIdToAddress:           make(map[uint64]string),
		dataStructureIdToRaftIds:  make(map[ID]map[uint64]bool),
		dataStructureIdToMetadata: make(map[ID]*pb.DataStructureMetadata),
		crush:                newCrush(),
		serfQueryRaftLimiers: &sync.Map{},
		logger:               logger,
		rehashDataStructureIdChan: make(chan *ID),
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

		for dsIdStr, metadata := range hi.DataStructureIdToMetadata {
			dsId, _ := parseIdFromString(dsIdStr)
			m.dataStructureIdToMetadata[*dsId] = metadata
			m.raftIdToAddress[metadata.RaftId] = addr
			raftIds, ok := m.dataStructureIdToRaftIds[*dsId]
			if !ok {
				raftIds = make(map[uint64]bool)
				m.dataStructureIdToRaftIds[*dsId] = raftIds
			}
			raftIds[metadata.RaftId] = false
			m.logger.Debugf("Added from [%s] dsId [%s] raft [%d %x] address [%s]", mem.Name, dsIdStr, metadata.RaftId, metadata.RaftId, addr)
		}

		m.crush.updatePeer(memberId, mem.Tags)
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

		for dsIdStr, metadata := range hi.DataStructureIdToMetadata {
			dsId, _ := parseIdFromString(dsIdStr)
			delete(m.dataStructureIdToMetadata, *dsId)
			delete(m.raftIdToAddress, metadata.RaftId)
			delete(m.dataStructureIdToRaftIds[*dsId], metadata.RaftId)
		}

		m.crush.removePeer(memberId, mem.Tags)
	}
}

func (m *membership) handleMemberFailedEvent(me serf.MemberEvent) {
	m.handleMemberLeaveEvent(me)
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
		m.logger.Warnf("Error processing query: %s", err.Error())
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
		// returning an error here will make the this serf node not send a response
		// back to the querying node which in the end could make the querying node
		// hang for the entire timeout if no nodes every answers
		return nil, fmt.Errorf("I don't know about raftId [%d] myself", req.RaftId)
	}

	resp := &pb.RaftIdQueryResponse{
		Peer: &pb.RaftPeer{
			RaftId:      req.RaftId,
			PeerAddress: addr,
		},
	}

	return resp.Marshal()
}

func (m *membership) handleDataStructureIdQuery(query *serf.Query) ([]byte, error) {
	req := &pb.DataStructureIdRequest{}
	err := req.Unmarshal(query.Payload)
	if err != nil {
		return nil, err
	}

	id, _ := parseIdFromProto(req.DataStructureId)
	peers := m.peersForDataStructureId(id)
	if peers == nil {
		// returning an error here will make the this serf node not send a response
		// back to the querying node which in the end could make the querying node
		// hang for the entire timeout if no nodes every answers
		return nil, fmt.Errorf("I don't know about data structure id [%s] myself", id)
	}

	resp := &pb.DataStructureIdResponse{
		Peers: peers,
	}

	return resp.Marshal()
}

func (m *membership) addDataStructureToRaftIdMapping(dataStructureId *ID, raftId uint64) error {
	m.serfTagMutex.Lock()
	defer m.serfTagMutex.Unlock()

	hi, err := m.getMyHostedItems()
	if err != nil {
		return err
	}

	// protobuf will optimize empty maps away and provoke a seg fault here
	if hi.DataStructureIdToMetadata == nil {
		hi.DataStructureIdToMetadata = make(map[string]*pb.DataStructureMetadata)
	}

	hi.DataStructureIdToMetadata[dataStructureId.String()] = &pb.DataStructureMetadata{
		RaftId:      raftId,
		NumReplicas: 3,
	}

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

	if hi.DataStructureIdToMetadata == nil {
		return nil
	}

	var dataStructureIdToDeleteStr string
	for dsIdStr, metadata := range hi.DataStructureIdToMetadata {
		if metadata.RaftId == raftId {
			dataStructureIdToDeleteStr = dsIdStr
			break
		}
	}
	delete(hi.DataStructureIdToMetadata, dataStructureIdToDeleteStr)

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

func (m *membership) findRaftWithId(raftId uint64) (*pb.RaftPeer, error) {
	req := &pb.RaftIdQueryRequest{RaftId: raftId}
	data, err := req.Marshal()
	if err != nil {
		return nil, err
	}

	serfQueryResp, err := m.serf.Query(strconv.Itoa(int(pb.RaftIdQuery)), data, m.serf.DefaultQueryParams())
	if err != nil {
		return nil, err
	}

	defer serfQueryResp.Close()
	serfRespCh := serfQueryResp.ResponseCh()
	m.logger.Debugf("Sent serf query: %s", req.String())

	for {
		select {
		case serfResp, ok := <-serfRespCh:
			if !ok {
				return nil, fmt.Errorf("Serf response channel was closed")
			}

			if serfResp.Payload != nil {
				resp := &pb.RaftIdQueryResponse{}
				err = resp.Unmarshal(serfResp.Payload)

				if err == nil {
					// this is pretty much free gossip
					// pick it up as we go
					// NB: assume we have the mutex already!!
					m.raftIdToAddress[resp.Peer.RaftId] = resp.Peer.PeerAddress
					m.logger.Debugf("Added query from [%s] raft [%d %x] address [%s]", serfResp.From, resp.Peer.RaftId, resp.Peer.RaftId, resp.Peer.PeerAddress)
					return resp.Peer, nil
				}
			}
		case <-time.After(time.Until(serfQueryResp.Deadline())):
			return nil, fmt.Errorf("Serf query timed out: %s", req.String())
		}
	}
}

func (m *membership) findDataStructureWithId(id *ID) (*pb.RaftPeer, error) {
	req := &pb.DataStructureIdRequest{DataStructureId: id.toProto()}
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
	m.logger.Debugf("Sent serf query: %s", req.String())

	for {
		select {
		case serfResp, ok := <-serfRespCh:
			if !ok {
				return nil, fmt.Errorf("Serf response channel was closed")
			}

			if serfResp.Payload != nil {
				resp := &pb.DataStructureIdResponse{}
				err = resp.Unmarshal(serfResp.Payload)

				if err == nil && len(resp.Peers) > 0 {
					// this is pretty much free gossip
					// pick it up as we go
					// NB: assume we have the mutex already!!
					for _, peer := range resp.Peers {
						m.raftIdToAddress[peer.RaftId] = peer.PeerAddress
						rafts, ok := m.dataStructureIdToRaftIds[*id]
						if !ok {
							rafts = make(map[uint64]bool)
							m.dataStructureIdToRaftIds[*id] = rafts
						}
						rafts[peer.RaftId] = true
						m.logger.Debugf("Added query from [%s] dsId [%s] raft [%d %x] address [%s]", serfResp.From, id.String(), peer.RaftId, peer.RaftId, peer.PeerAddress)
					}

					return resp.Peers[0], nil
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
	addr, ok := m.raftIdToAddress[raftId]
	// TODO - certainly this can be more intelligent
	// than just blocking everybody and just letting one person
	// per second in
	// I can at least use a map...
	if !ok || addr == "" {
		v, limOk := m.serfQueryRaftLimiers.Load(raftId)
		if !limOk {
			v, _ = m.serfQueryRaftLimiers.LoadOrStore(raftId, rate.NewLimiter(1, 1))
		}
		limiter := v.(*rate.Limiter)
		if limiter.Allow() {
			// if we don't find an address in our local cache,
			// we take a look
			go func() {
				// this method has as side-effect that internal maps
				// are being updated with lastest gossip
				m.findRaftWithId(raftId)
				m.serfQueryRaftLimiers.Delete(raftId)
			}()
		}
	}
	return addr
}

func (m *membership) getNumReplicasForDataStructure(id *ID) int {
	info, ok := m.dataStructureIdToMetadata[*id]
	if !ok {
		return -1
	}

	return int(info.NumReplicas)
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
func (m *membership) onePeerForDataStructureId(dataStructureId *ID) (*pb.RaftPeer, error) {
	m.serfTagMutex.Lock()
	defer m.serfTagMutex.Unlock()

	raftIdsMap, ok := m.dataStructureIdToRaftIds[*dataStructureId]
	// we got nothing in our local cache
	// let's make sure the entire cluster doesn't know about this data structure
	if !ok {
		peer, err := m.findDataStructureWithId(dataStructureId)
		if err != nil {
			return nil, err
		} else if peer == nil {
			return nil, fmt.Errorf("Can't find data structure with id [%s]", dataStructureId)
		}

		return peer, nil
	}

	m.logger.Warnf("Cache hit: %d", len(raftIdsMap))
	// cache hit!
	for raftId := range raftIdsMap {
		v, ok := m.raftIdToAddress[raftId]
		if ok {
			return &pb.RaftPeer{
				RaftId:      raftId,
				PeerAddress: v,
			}, nil
		}
	}

	return nil, fmt.Errorf("Can't find data structure with id [%d]", dataStructureId)
}

func (m *membership) peersForDataStructureId(dataStructureId *ID) []*pb.RaftPeer {
	raftIdsMap, ok := m.dataStructureIdToRaftIds[*dataStructureId]
	if !ok {
		m.logger.Infof("I don't know data structure with id [%s]", dataStructureId.String())
		return nil
	}

	peers := make([]*pb.RaftPeer, len(raftIdsMap))
	i := 0
	for raftId := range raftIdsMap {
		v, ok := m.raftIdToAddress[raftId]
		if ok {
			peers[i] = &pb.RaftPeer{
				RaftId:      raftId,
				PeerAddress: v,
			}
			i++
		}
	}
	return peers[:i]
}

func (m *membership) pickReplicaPeers(dataStructureId *ID, numReplicas int) []uint64 {
	wv := m.crush.place(dataStructureId, numReplicas, 1, 1)
	return wv.peerIds
}

// Result will look like this:
// [
//   {
//     "01CGWDXKV33BYBWK8MVNE8EHQQ": [
//       "8525546599457777599 7650d37640b867bf",
//       "6962446102838008331 609f93d787765a0b",
//       "14588314967425913900 ca741b2bb0ef942c"
//     ]
//   },
//   {
//     "14588314967425913900 ca741b2bb0ef942c": "address_3",
//     "6962446102838008331 609f93d787765a0b": "address_2",
//     "8525546599457777599 7650d37640b867bf": "address_1"
//   }
// ]
func (m *membership) marshalMembershipToJson() string {
	m.serfTagMutex.Lock()
	defer m.serfTagMutex.Unlock()

	dsIdToRaftIds := make(map[string][]string)
	for k, v := range m.dataStructureIdToRaftIds {
		a := make([]string, len(v))
		i := 0
		for raftId, _ := range v {
			a[i] = fmt.Sprintf("%d %x", raftId, raftId)
			i++
		}
		dsIdToRaftIds[k.String()] = a
	}

	raftIdToAddr := make(map[string]string)
	for k, v := range m.raftIdToAddress {
		key := fmt.Sprintf("%d %x", k, k)
		raftIdToAddr[key] = v
	}

	a := make([]interface{}, 2)
	a[0] = dsIdToRaftIds
	a[1] = raftIdToAddr

	bites, _ := json.MarshalIndent(a, "", "  ")
	return string(bites)
}

func (m *membership) stop() error {
	m.logger.Warn("Shutting down serf!")
	err := m.serf.Leave()
	if err != nil {
		m.logger.Errorf("Error leaving serf cluster: %s", err.Error())
	}
	return m.serf.Shutdown()
}
