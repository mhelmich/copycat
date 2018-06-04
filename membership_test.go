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
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/hashicorp/serf/serf"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestMembershipBasic(t *testing.T) {
	config := DefaultConfig()
	config.hostname = "127.0.0.1"
	config.CopyCatDataDir = "./test-TestMembershipBasic-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	m, err := newMembership(config)
	assert.Nil(t, err)

	err = m.addDataStructureToRaftIdMapping(123, 456)
	assert.Nil(t, err)

	err = m.stop()
	assert.Nil(t, err)
	err = os.RemoveAll(config.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestMembershipBasicTwoNodes(t *testing.T) {
	config1 := DefaultConfig()
	config1.hostname = "127.0.0.1"
	config1.CopyCatDataDir = "./test-TestMembershipBasicTwoNodes-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	m1, err := newMembership(config1)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.hostname = "127.0.0.1"
	config2.CopyCatDataDir = "./test-TestMembershipBasicTwoNodes-" + uint64ToString(randomRaftId()) + "/"
	config2.GossipPort = config1.GossipPort + 100
	config2.PeersToContact = make([]string, 1)
	config2.PeersToContact[0] = config1.hostname + ":" + strconv.Itoa(config1.GossipPort)
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	m2, err := newMembership(config2)
	assert.Nil(t, err)

	err = m1.stop()
	assert.Nil(t, err)
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = m2.stop()
	assert.Nil(t, err)
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestMembershipNodeJoin(t *testing.T) {
	m := &membership{
		memberIdToTags:           &sync.Map{},
		serfTagMutex:             &sync.Mutex{},
		raftIdToAddress:          make(map[uint64]string),
		dataStructureIdToRaftIds: make(map[uint64]map[uint64]bool),
		logger: log.WithFields(log.Fields{}),
	}

	memberId1 := uint64ToString(randomRaftId())
	hostname1 := "machine1"
	port1 := 9876
	mapping1 := make(map[uint64]uint64)
	mapping1[55] = 66
	hi1 := &pb.HostedItems{
		DataStructureToRaftMapping: mapping1,
	}
	hostedItems1 := proto.MarshalTextString(hi1)
	memberId2 := uint64ToString(randomRaftId())
	hostname2 := "machine2"
	port2 := 9877
	mapping2 := make(map[uint64]uint64)
	mapping2[55] = 88
	hi2 := &pb.HostedItems{
		DataStructureToRaftMapping: mapping2,
	}
	hostedItems2 := proto.MarshalTextString(hi2)

	ms := make([]serf.Member, 2)
	ms[0] = serf.Member{
		Name: memberId1,
		Tags: mockTags(hostname1, port1, hostedItems1),
	}
	ms[1] = serf.Member{
		Name: memberId2,
		Tags: mockTags(hostname2, port2, hostedItems2),
	}
	me := serf.MemberEvent{
		Type:    serf.EventMemberJoin,
		Members: ms,
	}
	m.handleMemberJoinEvent(me)

	tags, _ := m.memberIdToTags.Load(stringToUint64(ms[0].Name))
	assert.Equal(t, 3, len(tags.(map[string]string)))
	tags, _ = m.memberIdToTags.Load(stringToUint64(ms[1].Name))
	assert.Equal(t, 3, len(tags.(map[string]string)))
	assert.Equal(t, 2, len(m.raftIdToAddress))
	assert.Equal(t, "machine1:9876", m.raftIdToAddress[uint64(66)])
	assert.Equal(t, "machine2:9877", m.raftIdToAddress[uint64(88)])
	assert.Equal(t, "machine2:9877", m.getAddressForRaftId(uint64(88)))
	assert.Equal(t, 1, len(m.dataStructureIdToRaftIds))
	raftIdsForDS := m.dataStructureIdToRaftIds[uint64(55)]
	assert.Equal(t, 2, len(raftIdsForDS))
	for rId := range raftIdsForDS {
		_, ok := m.raftIdToAddress[rId]
		assert.True(t, ok)
	}
}

func TestMembershipHandleQuery(t *testing.T) {
	config := DefaultConfig()
	config.hostname = "127.0.0.1"
	config.CopyCatDataDir = "./test-TestMembershipHandleQuery-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	m, err := newMembership(config)
	assert.Nil(t, err)

	req := &pb.RaftIdQueryRequest{
		RaftId: uint64(999),
	}
	data, err := req.Marshal()
	assert.Nil(t, err)

	query := &serf.Query{
		Name:    strconv.Itoa(int(pb.RaftIdQuery)),
		Payload: data,
	}

	m.raftIdToAddress[req.RaftId] = "narf_narf_narf"
	m.handleQuery(query)

	err = m.stop()
	assert.Nil(t, err)
	err = os.RemoveAll(config.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestMembershipDataStructureQuery(t *testing.T) {
	config1 := DefaultConfig()
	config1.hostname = "127.0.0.1"
	config1.CopyCatDataDir = "./test-TestMembershipDataStructureQuery-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	m1, err := newMembership(config1)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.hostname = "127.0.0.1"
	config2.CopyCatDataDir = "./test-TestMembershipDataStructureQuery-" + uint64ToString(randomRaftId()) + "/"
	config2.GossipPort = config1.GossipPort + 10000
	config2.PeersToContact = make([]string, 1)
	config2.PeersToContact[0] = config1.hostname + ":" + strconv.Itoa(config1.GossipPort)
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	m2, err := newMembership(config2)
	assert.Nil(t, err)

	theAddressImLookingFor := "555_Fake_Street"
	randomDSId := randomRaftId()
	raftId1 := randomRaftId()
	m2.dataStructureIdToRaftIds[randomDSId] = make(map[uint64]bool)
	m2.dataStructureIdToRaftIds[randomDSId][raftId1] = false
	m2.raftIdToAddress[raftId1] = theAddressImLookingFor

	log.Infof("Querying for DS with id [%d]", randomDSId)
	peer, err := m1.findDataStructureWithId(randomDSId)
	assert.Nil(t, err)
	assert.NotNil(t, peer)
	log.Infof("Got response: %s", peer)
	assert.Equal(t, raftId1, peer.Id)
	assert.Equal(t, theAddressImLookingFor, peer.RaftAddress)

	err = m1.stop()
	assert.Nil(t, err)
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = m2.stop()
	assert.Nil(t, err)
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestMembershipPeersForDataStructure(t *testing.T) {
	m := &membership{
		raftIdToAddress:          make(map[uint64]string),
		dataStructureIdToRaftIds: make(map[uint64]map[uint64]bool),
		logger: log.WithFields(log.Fields{
			"test": "TestMembershipPeersForDataStructure",
		}),
	}

	myDataStructureId := randomRaftId()
	raftId1 := randomRaftId()
	raftId2 := randomRaftId()
	raftId3 := randomRaftId()

	address1 := "address_1"
	address2 := "address_2"
	address3 := "address_3"

	m.dataStructureIdToRaftIds[myDataStructureId] = make(map[uint64]bool)
	m.dataStructureIdToRaftIds[myDataStructureId][raftId1] = false
	m.dataStructureIdToRaftIds[myDataStructureId][raftId2] = false
	m.dataStructureIdToRaftIds[myDataStructureId][raftId3] = false

	m.raftIdToAddress[raftId1] = address1
	m.raftIdToAddress[raftId2] = address2
	m.raftIdToAddress[raftId3] = address3

	peers := m.peersForDataStructureId(myDataStructureId)
	assert.Equal(t, 3, len(peers))
	for _, peer := range peers {
		_, ok := m.raftIdToAddress[peer.Id]
		assert.True(t, ok)
	}

	peers = m.peersForDataStructureId(myDataStructureId + 3)
	assert.Equal(t, 0, len(peers))
}

func TestMembershipAddRemoveDataStructureToRaftIdMapping(t *testing.T) {
	config := DefaultConfig()
	config.hostname = "127.0.0.1"
	config.CopyCatDataDir = "./test-TestMembershipAddRemoveDataStructureToRaftIdMapping-" + uint64ToString(randomRaftId()) + "/"
	err := os.MkdirAll(config.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	m, err := newMembership(config)
	assert.Nil(t, err)

	dataStructureId := uint64(123)
	raftId := uint64(456)

	err = m.addDataStructureToRaftIdMapping(dataStructureId, raftId)
	assert.Nil(t, err)

	tags := m.serf.LocalMember().Tags
	hostedItems := tags[serfMDKeyHostedItems]
	hi := &pb.HostedItems{}
	err = proto.UnmarshalText(hostedItems, hi)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(hi.DataStructureToRaftMapping))
	raftIdIRead, ok := hi.DataStructureToRaftMapping[dataStructureId]
	assert.True(t, ok)
	assert.Equal(t, raftId, raftIdIRead)

	err = m.removeDataStructureToRaftIdMapping(raftId)
	assert.Nil(t, err)

	tags = m.serf.LocalMember().Tags
	hostedItems = tags[serfMDKeyHostedItems]
	hi = &pb.HostedItems{}
	err = proto.UnmarshalText(hostedItems, hi)
	assert.Nil(t, err)
	assert.Equal(t, 0, len(hi.DataStructureToRaftMapping))
	_, ok = hi.DataStructureToRaftMapping[dataStructureId]
	assert.False(t, ok)

	err = m.stop()
	assert.Nil(t, err)
	err = os.RemoveAll(config.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestMembershipPickFromMetadata(t *testing.T) {
	picker := func(peerId uint64, tags map[string]string) bool {
		return true
	}

	m := &membership{
		memberIdToTags: &sync.Map{},
	}

	nodeId1 := randomRaftId()
	nodeId2 := randomRaftId()
	m.memberIdToTags.Store(nodeId1, make(map[string]string))
	m.memberIdToTags.Store(nodeId2, make(map[string]string))

	picked := m.pickFromMetadata(picker, 2, make([]uint64, 0))
	assert.Equal(t, 2, len(picked))

	misfits := make([]uint64, 1)
	misfits[0] = nodeId1
	picked = m.pickFromMetadata(picker, 2, misfits)
	assert.Equal(t, 1, len(picked))
}

func mockTags(host string, port int, hostedItems string) map[string]string {
	m := make(map[string]string)
	m[serfMDKeyHost] = host
	m[serfMDKeyCopyCatPort] = strconv.Itoa(port)
	m[serfMDKeyHostedItems] = hostedItems
	return m
}
