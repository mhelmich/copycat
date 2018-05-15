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

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	log "github.com/sirupsen/logrus"
)

const (
	defaultCopyCatPort    = 11983
	defaultCopyCatDataDir = "./copycat.db/"
)

func DefaultConfig() *CopyCatConfig {
	host, _ := os.Hostname()
	return &CopyCatConfig{
		hostname:       host,
		CopyCatPort:    defaultCopyCatPort,
		gossipPort:     defaultCopyCatPort + 1000,
		CopyCatDataDir: defaultCopyCatDataDir,
		logger: log.WithFields(log.Fields{
			"component": "copycat",
			"host":      host,
			"port":      strconv.Itoa(defaultCopyCatPort),
		}),
	}
}

type CopyCatConfig struct {
	CopyCatPort    int
	CopyCatDataDir string
	PeersToContact []string

	hostname   string
	gossipPort int
	logger     *log.Entry
}

func NewCopyCat(config *CopyCatConfig) (CopyCat, error) {
	return newCopyCat(config)
}

// CopyCat is the struct the consumer needs to create
// in order to create distributed data structures
type CopyCat interface {
	LoadDataStructure(id uint64, ds DataStructure) (chan<- []byte, <-chan []byte, <-chan error)
	Shutdown()
}

// DataStructure Every data structure that uses CopyCat needs to
// implement this interface
type DataStructure interface {
	GetSnapshot() ([]byte, error)
	SetSnapshot([]byte) error
}

type transport interface {
	addPeer(id uint64, addr string) error
	removePeer(id uint64)
	sendMessages(msgs []raftpb.Message)
	errorChannel() chan error
	stop()
}

type store interface {
	raft.Storage
	saveConfigState(confState raftpb.ConfState) error
	saveEntriesAndState(entries []raftpb.Entry, hardState raftpb.HardState) error
	saveSnap(snap raftpb.Snapshot) error
	close()
}
