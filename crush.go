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
	"crypto/hmac"
	"crypto/sha512"
	"sync"
)

// This simple implementation of the crush algorithm was inspired by this reading:
// https://www.slideshare.net/sageweil1/a-crash-course-in-crush
// https://ceph.com/wp-content/uploads/2016/08/weil-crush-sc06.pdf
// https://ceph.com/wp-content/uploads/2016/08/weil-thesis.pdf

func newCrush() *crush {
	return &crush{
		allDataCenters:    &sync.Map{},
		numHashingRetries: 17,
	}
}

// basic and limited crush implementation
// Limitations include:
// - hard-coded number of hierarchies
// - hard-coded placement rule that only allows configuring the number of replicas on each level
// - hard-coded hashinig algorithmn (hmac sha-512 with rendezvous hashing)
// - no weighted tree (or rather: tree with equal weights)
type crush struct {
	// This struct encapsulates the madness of building and walking a tree-like hierarchy
	// made up out of the current topology of peers.
	// The hierarchy consists of (technically) three levels:
	// 1. all data centers
	// 2. all racks
	// 3. all nodes
	// This hierarchy is built with three layers of maps:
	// 1. all data centers               => dc name (string) -> *sync.Map
	// 2. all racks within a data center => rack name (string) -> *sync.Map
	// 3. all peers within a rack        => peer id (uint64) -> bool
	// These maps will be dynamically created as nodes in respective
	// locations will be observed.
	allDataCenters    *sync.Map
	numHashingRetries uint8
}

// this method uses a crypto "save" random to create a hash
// therefore I *should* be able to achieve a uniform distribution
// r is a counter to modify the hash during each iteration
// dataStructureId is the unique id of the data structure we're trying to place
// hierarchy is either a string (data center / rack) or a uint64 (peer id)
func (c *crush) hash(r uint8, dataStructureId ID, hierarchyId []byte) []byte {
	h := hmac.New(sha512.New512_256, []byte("tag"))
	buf := make([]byte, 1)
	buf[0] = byte(r)
	h.Write(buf)
	h.Write(dataStructureId[:])
	h.Write(hierarchyId)
	return h.Sum(nil)
}

func (c *crush) hashRendezVous(r uint8, level *sync.Map, dataStructureId ID) interface{} {
	var highestScore []byte
	var highestKey interface{}

	level.Range(func(key, value interface{}) bool {
		var bites []byte
		switch key.(type) {
		case string:
			// this is either the rack or data center name
			bites = []byte(key.(string))
		case uint64:
			// this has gotta be a peer id
			bites = uint64ToBytes(key.(uint64))
		}
		score := c.hash(r, dataStructureId, bites)
		// lexiographically compare both hashes and keep the higher one
		if highestScore == nil || bytes.Compare(highestScore, score) > 0 {
			highestScore = score
			highestKey = key
		}
		return true
	})

	return highestKey
}

func (c *crush) dfs(m *sync.Map, wv *workingVector, idxInWorkingVector int) bool {
	if idxInWorkingVector >= len(wv.numReplicas) {
		return false
	}

	numItemsNeededOnThisLevel := wv.numReplicas[idxInWorkingVector]
	intermediateResults := make([]interface{}, numItemsNeededOnThisLevel)
	idxRes := 0
	var r uint8
	for ; idxRes < numItemsNeededOnThisLevel && r < c.numHashingRetries; r++ {
		keyIntoM := c.hashRendezVous(r, m, wv.dataStructureId)
		if c.canUse(intermediateResults, keyIntoM) {
			switch keyIntoM.(type) {
			case string:
				var added bool
				// this is either the name of a data center or rack
				dataCenterOrRackName := keyIntoM.(string)
				// see whether there's a subtree for this dc/rack
				v, ok := m.Load(dataCenterOrRackName)
				if ok {
					m := v.(*sync.Map)
					// recurse and descend
					added = c.dfs(m, wv, idxInWorkingVector+1)
				}

				// if added is true, we could fulfill the demands
				// of the entire subtree ... great!
				// add it to the intermediate results
				// and let's move on
				if added {
					intermediateResults[idxRes] = dataCenterOrRackName
					idxRes++
				}
			case uint64:
				// we are on peer level
				// no more descending
				peerId := keyIntoM.(uint64)
				intermediateResults[idxRes] = peerId
				idxRes++
			}
		}
	}

	// this clause declares utmost strictness!!
	// if we can't exactly fulfill the ask,
	// we abondon this entire branch
	// if we want to be more lenient, we can do something like this:
	// if idxRes >= 0 { ...
	if idxRes >= numItemsNeededOnThisLevel {
		wv.intermediateResults[idxInWorkingVector] = append(wv.intermediateResults[idxInWorkingVector], intermediateResults...)
		return true
	}

	// we didn't find as much as we need
	// we return false and abandon this entire branch
	return false
}

func (c *crush) place(dataStructureId ID, numDataCenterReplicas int, numRackReplicasInDataCenter int, numPeersReplicasInRack int) *workingVector {
	wv := &workingVector{
		dataStructureId:     dataStructureId,
		numReplicas:         []int{numDataCenterReplicas, numRackReplicasInDataCenter, numPeersReplicasInRack},
		intermediateResults: make([][]interface{}, 3),
	}

	// kick off recursion
	complete := c.dfs(c.allDataCenters, wv, 0)
	lastLevel := wv.intermediateResults[len(wv.intermediateResults)-1]
	wv.peerIds = make([]uint64, len(lastLevel))
	if complete {
		for idx, o := range lastLevel {
			wv.peerIds[idx] = o.(uint64)
		}
	} else {
		idx := 0
		for _, o := range lastLevel {
			if o != nil {
				wv.peerIds[idx] = o.(uint64)
				idx++
			}
		}
		wv.peerIds = wv.peerIds[:idx]
	}
	return wv
}

// right now canUse only checks whether this peer has been selected already
// in the future we could probably double-check whether the peer is healthy or has space or whatever
func (c *crush) canUse(chosenKeys []interface{}, newKey interface{}) bool {
	switch newKey.(type) {
	case string:
		n := newKey.(string)
		for _, s := range chosenKeys {
			if s != nil && s.(string) == n {
				return false
			}
		}
		return true
	case uint64:
		n := newKey.(uint64)
		for _, i := range chosenKeys {
			if i != nil && i.(uint64) == n {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// this is called by the gossip component repeatedly but never concurrently
func (c *crush) updatePeer(peerId uint64, metadata map[string]string) {
	dc := metadata[serfMDKeyDataCenter]
	rack := metadata[serfMDKeyRack]

	m1, ok := c.allDataCenters.Load(dc)
	if !ok {
		newMap := &sync.Map{}
		m1, _ = c.allDataCenters.LoadOrStore(dc, newMap)
	}

	dcMap := m1.(*sync.Map)
	m2, ok := dcMap.Load(rack)
	if !ok {
		newMap := &sync.Map{}
		m2, _ = dcMap.LoadOrStore(rack, newMap)
	}

	rackMap := m2.(*sync.Map)
	rackMap.Store(peerId, true)
}

// this is called by the gossip component repeatedly but never concurrently
func (c *crush) removePeer(peerId uint64, metadata map[string]string) {
	dc := metadata[serfMDKeyDataCenter]
	rack := metadata[serfMDKeyRack]

	m1, ok := c.allDataCenters.Load(dc)
	if !ok {
		return
	}

	dcMap := m1.(*sync.Map)
	m2, ok := dcMap.Load(rack)
	if !ok {
		return
	}

	rackMap := m2.(*sync.Map)
	rackMap.Delete(peerId)

	if isSyncMapEmpty(rackMap) {
		dcMap.Delete(rack)
	}

	if isSyncMapEmpty(dcMap) {
		c.allDataCenters.Delete(dc)
	}
}

// this struct somewhat represents a placement trace
// it keeps all intermediate steps
// the only interesting end result for prod use is the pickedPeers slice
type workingVector struct {
	dataStructureId     ID
	numReplicas         []int
	intermediateResults [][]interface{}
	peerIds             []uint64
}
