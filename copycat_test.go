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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCopyCatBasic(t *testing.T) {
	config1 := DefaultConfig()
	config1.CopyCatDataDir = "./test-TestCopyCatBasic-" + uint64ToString(randomRaftId())
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc1, err := newCopyCat(config1)
	assert.Nil(t, err)

	cc1.Shutdown()
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestCopyCatNewDataStructure(t *testing.T) {
	newDataStructureId := randomRaftId()
	config1 := DefaultConfig()
	config1.CopyCatDataDir = "./test-TestCopyCatNewDataStructure-" + uint64ToString(randomRaftId())
	config1.GossipPort = config1.GossipPort + 22222
	config1.CopyCatPort = config1.CopyCatPort + 22222
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc1, err := newCopyCat(config1)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.CopyCatDataDir = "./test-TestCopyCatNewDataStructure-" + uint64ToString(randomRaftId())
	config2.GossipPort = config1.GossipPort + 10000
	config2.CopyCatPort = config1.CopyCatPort + 10000
	config2.PeersToContact = make([]string, 1)
	config2.PeersToContact[0] = config1.hostname + ":" + strconv.Itoa(config1.GossipPort)
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc2, err := newCopyCat(config2)
	assert.Nil(t, err)

	peers, err := cc1.choosePeersForNewDataStructure(newDataStructureId, cc1.membership.getAllMetadata(), 1)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(peers))
	assert.Equal(t, config2.hostname+":"+strconv.Itoa(config2.CopyCatPort), peers[0].RaftAddress)

	cc1.Shutdown()
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)

	cc2.Shutdown()
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
}
