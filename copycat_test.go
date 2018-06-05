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
	config1.CopyCatDataDir = "./test-TestCopyCatBasic-" + uint64ToString(randomRaftId()) + "/"
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
	config1.Hostname = "127.0.0.1"
	config1.CopyCatDataDir = "./test-TestCopyCatNewDataStructure-" + uint64ToString(randomRaftId()) + "/"
	config1.GossipPort = 10000
	config1.CopyCatPort = 20000
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc1, err := newCopyCat(config1)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.Hostname = "127.0.0.1"
	config2.CopyCatDataDir = "./test-TestCopyCatNewDataStructure-" + uint64ToString(randomRaftId()) + "/"
	config2.GossipPort = config1.GossipPort + 1111
	config2.CopyCatPort = config1.CopyCatPort + 1111
	config2.PeersToContact = make([]string, 1)
	config2.PeersToContact[0] = config1.Hostname + ":" + strconv.Itoa(config1.GossipPort)
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc2, err := newCopyCat(config2)
	assert.Nil(t, err)

	proposeChan1, commitChan1, _, _, err := cc1.ConnectToDataStructure(newDataStructureId, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)

	proposeChan1 <- []byte("hello_world")
	bites := <-commitChan1
	assert.Equal(t, "hello_world", string(bites))

	cc1.Shutdown()
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)

	cc2.Shutdown()
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestCopyCatConnectToExistingDataStructure(t *testing.T) {
	newDataStructureId := randomRaftId()
	config1 := DefaultConfig()
	config1.Hostname = "127.0.0.1"
	config1.CopyCatDataDir = "./test-TestCopyCatConnectToExistingDataStructure-" + uint64ToString(randomRaftId()) + "/"
	config1.GossipPort = 10000
	config1.CopyCatPort = 20000
	err := os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc1, err := newCopyCat(config1)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.Hostname = "127.0.0.1"
	config2.CopyCatDataDir = "./test-TestCopyCatConnectToExistingDataStructure-" + uint64ToString(randomRaftId()) + "/"
	config2.GossipPort = config1.GossipPort + 1111
	config2.CopyCatPort = config1.CopyCatPort + 1111
	config2.PeersToContact = make([]string, 1)
	config2.PeersToContact[0] = config1.Hostname + ":" + strconv.Itoa(config1.GossipPort)
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc2, err := newCopyCat(config2)
	assert.Nil(t, err)

	proposeChan1, commitChan1, _, _, err := cc1.ConnectToDataStructure(newDataStructureId, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)

	proposeChan1 <- []byte("hello")
	bites := <-commitChan1
	assert.Equal(t, "hello", string(bites))
	proposeChan1 <- []byte("world")
	bites = <-commitChan1
	assert.Equal(t, "world", string(bites))

	config3 := DefaultConfig()
	config3.Hostname = "127.0.0.1"
	config3.CopyCatDataDir = "./test-TestCopyCatConnectToExistingDataStructure-" + uint64ToString(randomRaftId()) + "/"
	config3.GossipPort = config2.GossipPort + 1111
	config3.CopyCatPort = config2.CopyCatPort + 1111
	config3.PeersToContact = make([]string, 1)
	config3.PeersToContact[0] = config1.Hostname + ":" + strconv.Itoa(config1.GossipPort)
	err = os.MkdirAll(config3.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc3, err := newCopyCat(config3)
	assert.Nil(t, err)

	_, commitChan3, _, _, err := cc3.ConnectToDataStructure(newDataStructureId, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	bites = <-commitChan3
	assert.Equal(t, "hello", string(bites))
	bites = <-commitChan3
	assert.Equal(t, "world", string(bites))

	cc1.Shutdown()
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)

	cc2.Shutdown()
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)

	cc3.Shutdown()
	err = os.RemoveAll(config3.CopyCatDataDir)
	assert.Nil(t, err)
}
