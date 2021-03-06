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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCopyCatBasic(t *testing.T) {
	config1 := DefaultConfig()
	config1.CopyCatDataDir = "./test-TestCopyCatBasic/"
	err := os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc1, err := newCopyCat(config1)
	assert.Nil(t, err)

	cc1.Shutdown()
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestCopyCatNewDataStructure(t *testing.T) {
	config1 := DefaultConfig()
	config1.Hostname = "127.0.0.1"
	config1.CopyCatDataDir = "./test-TestCopyCatNewDataStructure/"
	config1.GossipPort = 10000
	config1.CopyCatPort = 20000
	err := os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc1, err := newCopyCat(config1)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.Hostname = "127.0.0.1"
	config2.CopyCatDataDir = "./test-TestCopyCatNewDataStructure/"
	config2.GossipPort = config1.GossipPort + 1111
	config2.CopyCatPort = config1.CopyCatPort + 1111
	config2.PeersToContact = make([]string, 1)
	config2.PeersToContact[0] = config1.Hostname + ":" + strconv.Itoa(config1.GossipPort)
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc2, err := newCopyCat(config2)
	assert.Nil(t, err)

	newDataStructureId, err := cc1.NewDataStructureID()
	assert.Nil(t, err)
	proposeChan1, commitChan1, _, _, err := cc1.SubscribeToDataStructure(newDataStructureId, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)

	proposeChan1 <- []byte("hello_world")
	bites := <-commitChan1
	assert.Equal(t, "hello_world", string(bites))
	// close channel to close interactive raft backing the channel
	close(proposeChan1)

	cc1.Shutdown()
	err = os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)

	cc2.Shutdown()
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
}

func TestCopyCatSubscribeToExistingDataStructure(t *testing.T) {
	config1 := DefaultConfig()
	config1.Hostname = "127.0.0.1"
	config1.CopyCatDataDir = "./test-TestCopyCatSubscribeToExistingDataStructure/"
	config1.GossipPort = 10000
	config1.CopyCatPort = 20000
	err := os.RemoveAll(config1.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.MkdirAll(config1.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc1, err := newCopyCat(config1)
	assert.Nil(t, err)

	config2 := DefaultConfig()
	config2.Hostname = "127.0.0.1"
	config2.CopyCatDataDir = "./test-TestCopyCatSubscribeToExistingDataStructure/"
	config2.GossipPort = config1.GossipPort + 1111
	config2.CopyCatPort = config1.CopyCatPort + 1111
	config2.PeersToContact = make([]string, 1)
	config2.PeersToContact[0] = config1.Hostname + ":" + strconv.Itoa(config1.GossipPort)
	err = os.RemoveAll(config2.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.MkdirAll(config2.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc2, err := newCopyCat(config2)
	assert.Nil(t, err)

	newDataStructureId, err := cc1.NewDataStructureID()
	assert.Nil(t, err)
	proposeChan1, commitChan1, _, _, err := cc1.SubscribeToDataStructure(newDataStructureId, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)

	proposeChan1 <- []byte("hello")
	bites := <-commitChan1
	assert.Equal(t, "hello", string(bites))
	proposeChan1 <- []byte("world")
	bites = <-commitChan1
	assert.Equal(t, "world", string(bites))

	config3 := DefaultConfig()
	config3.Hostname = "127.0.0.1"
	config3.CopyCatDataDir = "./test-TestCopyCatSubscribeToExistingDataStructure/"
	config3.GossipPort = config2.GossipPort + 1111
	config3.CopyCatPort = config2.CopyCatPort + 1111
	config3.PeersToContact = make([]string, 1)
	config3.PeersToContact[0] = config1.Hostname + ":" + strconv.Itoa(config1.GossipPort)
	err = os.RemoveAll(config3.CopyCatDataDir)
	assert.Nil(t, err)
	err = os.MkdirAll(config3.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)
	cc3, err := newCopyCat(config3)
	assert.Nil(t, err)

	proposeChan3, commitChan3, _, _, err := cc3.SubscribeToDataStructure(newDataStructureId, func() ([]byte, error) { return make([]byte, 0), nil })
	assert.Nil(t, err)
	bites = <-commitChan3
	assert.Equal(t, "hello", string(bites))
	bites = <-commitChan3
	assert.Equal(t, "world", string(bites))

	close(proposeChan1)
	close(proposeChan3)

	// HACK - yupp...shutting down the underlying store is aync :)
	// that's why removing the folder fails sometimes
	// I can live with this ugliness for now

	cc3.Shutdown()
	err = os.RemoveAll(config3.CopyCatDataDir)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(50 * time.Millisecond)
		err = os.RemoveAll(config1.CopyCatDataDir)
	}
	assert.Nil(t, err)

	cc1.Shutdown()
	err = os.RemoveAll(config1.CopyCatDataDir)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(50 * time.Millisecond)
		err = os.RemoveAll(config1.CopyCatDataDir)
	}
	assert.Nil(t, err)

	cc2.Shutdown()
	err = os.RemoveAll(config2.CopyCatDataDir)
	for i := 0; i < 3 && err != nil; i++ {
		time.Sleep(50 * time.Millisecond)
		err = os.RemoveAll(config1.CopyCatDataDir)
	}
	assert.Nil(t, err)
}
