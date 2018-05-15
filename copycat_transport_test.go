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
	"testing"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestTransportBasic(t *testing.T) {
	config := DefaultConfig()
	config.CopyCatDataDir = "./test-" + uint64ToString(randomRaftId())
	err := os.MkdirAll(config.CopyCatDataDir, os.ModePerm)
	assert.Nil(t, err)

	m := &membership{
		memberIdToTags:           make(map[uint64]map[string]string),
		raftIdToAddress:          make(map[uint64]string),
		dataStructureIdToRaftIds: make(map[uint64]map[uint64]bool),
		logger: log.WithFields(log.Fields{}),
	}

	transport, err := newTransport(config, m)
	assert.Nil(t, err)

	transport.stop()
	err = os.RemoveAll(config.CopyCatDataDir)
	assert.Nil(t, err)
}
