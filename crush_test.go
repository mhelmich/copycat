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
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestCrushBasic(t *testing.T) {
	nodeId1 := randomRaftId()
	m1 := make(map[string]string)
	m1[serfMDKeyDataCenter] = "dc1"
	m1[serfMDKeyRack] = "rack1"

	nodeId2 := randomRaftId()
	m2 := make(map[string]string)
	m2[serfMDKeyDataCenter] = "dc2"
	m2[serfMDKeyRack] = "rack2"

	c := newCrush()
	c.updatePeer(nodeId1, m1)
	c.updatePeer(nodeId2, m2)
	// two data centers
	assert.Equal(t, 2, sizeOfMap(c.allDataCenters))
	v1, ok := c.allDataCenters.Load(m1[serfMDKeyDataCenter])
	assert.True(t, ok)
	dc1Map := v1.(*sync.Map)
	// but only one rack inside the data center
	assert.Equal(t, 1, sizeOfMap(dc1Map))
	v2, ok := dc1Map.Load(m1[serfMDKeyRack])
	assert.True(t, ok)
	rack1Map := v2.(*sync.Map)
	_, ok = rack1Map.Load(nodeId1)
	assert.True(t, ok)
}

// this test is testing that the algorithm is stable and repeatable
// same input => same output
func TestCrushOutputStable(t *testing.T) {
	for i := 0; i < 7; i++ {
		TestCrushAddAndEasyPlace(t)
	}
}

func TestCrushAddAndEasyPlace(t *testing.T) {
	peerId1 := uint64(7412850045943536150)
	m1 := make(map[string]string)
	m1[serfMDKeyDataCenter] = "dc1"
	m1[serfMDKeyRack] = "rack1"

	nodeId2 := uint64(17913124395087457087)
	m2 := make(map[string]string)
	m2[serfMDKeyDataCenter] = "dc1"
	m2[serfMDKeyRack] = "rack2"

	peerId2 := uint64(10022980431353450773)
	m3 := make(map[string]string)
	m3[serfMDKeyDataCenter] = "dc2"
	m3[serfMDKeyRack] = "rack1"

	peerId4 := uint64(33937148811588088)
	m4 := make(map[string]string)
	m4[serfMDKeyDataCenter] = "dc2"
	m4[serfMDKeyRack] = "rack2"

	inDC1 := make(map[uint64]bool)
	inDC1[peerId1] = true
	inDC1[nodeId2] = true
	inDC2 := make(map[uint64]bool)
	inDC2[peerId2] = true
	inDC2[peerId4] = true

	crush := newCrush()
	crush.updatePeer(peerId1, m1)
	crush.updatePeer(nodeId2, m2)
	crush.updatePeer(peerId2, m3)
	crush.updatePeer(peerId4, m4)

	dsId, err := parseIdFromString("01CG0986V8JPXVER781B4K9CQ0")
	assert.Nil(t, err)
	wv := crush.place(dsId, 2, 1, 1)

	assert.Equal(t, 3, len(wv.intermediateResults))
	assert.Equal(t, 2, len(wv.intermediateResults[0]))
	log.Infof("%v", wv.intermediateResults[0])
	assert.Equal(t, 2, len(wv.intermediateResults[1]))
	log.Infof("%v", wv.intermediateResults[1])
	assert.Equal(t, 2, len(wv.intermediateResults[2]))
	log.Infof("%v", wv.intermediateResults[2])

	pickedDC1 := wv.intermediateResults[0][0].(string)
	pickedDC2 := wv.intermediateResults[0][1].(string)
	assert.Equal(t, "dc2", pickedDC1)
	assert.Equal(t, "dc1", pickedDC2)

	pickedRack1 := wv.intermediateResults[1][0].(string)
	pickedRack2 := wv.intermediateResults[1][1].(string)
	assert.Equal(t, "rack2", pickedRack1)
	assert.Equal(t, "rack2", pickedRack2)

	pickedPeer1 := wv.intermediateResults[2][0].(uint64)
	pickedPeer2 := wv.intermediateResults[2][1].(uint64)
	assert.Equal(t, peerId4, pickedPeer1)
	assert.Equal(t, nodeId2, pickedPeer2)
}

func TestCrushAddAndPlaceWithBacktracking(t *testing.T) {
	peerId1 := uint64(12684344403371699555)
	m1 := make(map[string]string)
	m1[serfMDKeyDataCenter] = "dc1"
	m1[serfMDKeyRack] = "rack1"

	peerId2 := uint64(11791432141206284128)
	m2 := make(map[string]string)
	m2[serfMDKeyDataCenter] = "dc1"
	m2[serfMDKeyRack] = "rack2"

	peerId3 := uint64(17544167887397674930)
	m3 := make(map[string]string)
	m3[serfMDKeyDataCenter] = "dc2"
	m3[serfMDKeyRack] = "rack1"

	peerId4 := uint64(11904810957279699965)
	m4 := make(map[string]string)
	m4[serfMDKeyDataCenter] = "dc2"
	m4[serfMDKeyRack] = "rack2"

	peerId5 := uint64(12553428326992971126)
	m5 := make(map[string]string)
	m5[serfMDKeyDataCenter] = "dc1"
	m5[serfMDKeyRack] = "rack1"

	peerId6 := uint64(8631303188346071915)
	m6 := make(map[string]string)
	m6[serfMDKeyDataCenter] = "dc1"
	m6[serfMDKeyRack] = "rack1"

	peerId7 := uint64(3558659569050494294)
	m7 := make(map[string]string)
	m7[serfMDKeyDataCenter] = "dc2"
	m7[serfMDKeyRack] = "rack1"

	peerId8 := uint64(1102927419575921984)
	m8 := make(map[string]string)
	m8[serfMDKeyDataCenter] = "dc2"
	m8[serfMDKeyRack] = "rack1"

	inDC1 := make(map[uint64]bool)
	inDC1[peerId1] = true
	inDC1[peerId2] = true
	inDC1[peerId5] = true
	inDC1[peerId6] = true
	inDC2 := make(map[uint64]bool)
	inDC2[peerId3] = true
	inDC2[peerId4] = true
	inDC2[peerId7] = true
	inDC2[peerId8] = true

	crush := newCrush()
	crush.updatePeer(peerId1, m1)
	crush.updatePeer(peerId2, m2)
	crush.updatePeer(peerId3, m3)
	crush.updatePeer(peerId4, m4)
	crush.updatePeer(peerId5, m5)
	crush.updatePeer(peerId6, m6)
	crush.updatePeer(peerId7, m7)
	crush.updatePeer(peerId8, m8)

	dsId, err := parseIdFromString("01CG0911H1FDNH07NVDDGCYMEW")
	assert.Nil(t, err)
	// the clue of this test is that for this input
	// the algorithm wants to place replicas on rack2 first (because that's what it hashes to)
	// then the algorithm backtracks and descends into rack1
	// because that's the only rack it can find two peers in
	// all rack2s have only one peer
	wv := crush.place(dsId, 2, 1, 2)

	assert.Equal(t, 3, len(wv.intermediateResults))
	assert.Equal(t, 2, len(wv.intermediateResults[0]))
	log.Infof("%v", wv.intermediateResults[0])
	assert.Equal(t, 2, len(wv.intermediateResults[1]))
	log.Infof("%v", wv.intermediateResults[1])
	assert.Equal(t, 4, len(wv.intermediateResults[2]))
	log.Infof("%v", wv.intermediateResults[2])

	pickedDC1 := wv.intermediateResults[0][0].(string)
	pickedDC2 := wv.intermediateResults[0][1].(string)
	assert.Equal(t, "dc2", pickedDC1)
	assert.Equal(t, "dc1", pickedDC2)

	pickedRack1 := wv.intermediateResults[1][0].(string)
	pickedRack2 := wv.intermediateResults[1][1].(string)
	assert.Equal(t, "rack1", pickedRack1)
	assert.Equal(t, "rack1", pickedRack2)

	pickedPeer1 := wv.intermediateResults[2][0].(uint64)
	pickedPeer2 := wv.intermediateResults[2][1].(uint64)
	pickedPeer3 := wv.intermediateResults[2][2].(uint64)
	pickedPeer4 := wv.intermediateResults[2][3].(uint64)
	assert.Equal(t, peerId7, pickedPeer1)
	assert.Equal(t, peerId8, pickedPeer2)
	assert.Equal(t, peerId6, pickedPeer3)
	assert.Equal(t, peerId1, pickedPeer4)
}

func TestUpdateRemovePeer(t *testing.T) {
	peerId1 := uint64(12684344403371699555)
	m1 := make(map[string]string)
	m1[serfMDKeyDataCenter] = "dc1"
	m1[serfMDKeyRack] = "rack1"

	peerId2 := uint64(11791432141206284128)
	m2 := make(map[string]string)
	m2[serfMDKeyDataCenter] = "dc1"
	m2[serfMDKeyRack] = "rack2"

	crush := newCrush()
	crush.updatePeer(peerId1, m1)
	crush.updatePeer(peerId2, m2)
	assert.Equal(t, 1, sizeOfMap(crush.allDataCenters))
	// verify dc1
	v, ok := crush.allDataCenters.Load(m1[serfMDKeyDataCenter])
	assert.True(t, ok)
	dc1Map := v.(*sync.Map)
	assert.Equal(t, 2, sizeOfMap(dc1Map))
	// verify rack1
	v, ok = dc1Map.Load(m1[serfMDKeyRack])
	assert.True(t, ok)
	rack1Map := v.(*sync.Map)
	assert.Equal(t, 1, sizeOfMap(rack1Map))
	// verify rack2
	v, ok = dc1Map.Load(m2[serfMDKeyRack])
	assert.True(t, ok)
	rack2Map := v.(*sync.Map)
	assert.Equal(t, 1, sizeOfMap(rack2Map))
	// verify peer1
	_, ok = rack1Map.Load(peerId1)
	assert.True(t, ok)
	// verify peer2
	_, ok = rack2Map.Load(peerId2)
	assert.True(t, ok)

	// remove peer 1
	crush.removePeer(peerId1, m1)
	assert.Equal(t, 1, sizeOfMap(crush.allDataCenters))
	// verify dc1
	v, ok = crush.allDataCenters.Load(m2[serfMDKeyDataCenter])
	assert.True(t, ok)
	dc1Map = v.(*sync.Map)
	assert.Equal(t, 1, sizeOfMap(dc1Map))
	// verify rack2
	v, ok = dc1Map.Load(m2[serfMDKeyRack])
	assert.True(t, ok)
	rack2Map = v.(*sync.Map)
	assert.Equal(t, 1, sizeOfMap(rack2Map))
	// verify peer2
	_, ok = rack2Map.Load(peerId2)
	assert.True(t, ok)

	// remove peer 1 again
	crush.removePeer(peerId1, m1)
	assert.Equal(t, 1, sizeOfMap(crush.allDataCenters))

	// remove peer 2
	crush.removePeer(peerId2, m2)
	assert.Equal(t, 0, sizeOfMap(crush.allDataCenters))

	// remove peer 2 again
	crush.removePeer(peerId2, m2)
	assert.Equal(t, 0, sizeOfMap(crush.allDataCenters))
}
