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
	"context"
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

func DefaultConfig() *Config {
	host, _ := os.Hostname()
	return &Config{
		hostname:       host,
		CopyCatPort:    defaultCopyCatPort,
		GossipPort:     defaultCopyCatPort + 1000,
		CopyCatDataDir: defaultCopyCatDataDir,
		Location:       "",
		logger: log.WithFields(log.Fields{
			"component": "copycat",
			"host":      host,
			"port":      strconv.Itoa(defaultCopyCatPort),
		}),
	}
}

// Config is the public configuration struct that needs to be passed into the constructor function.
type Config struct {
	// Port of the CopyCat server. The server runs the management interface and the raft state machine.
	CopyCatPort int
	// Port on which CopyCat gossips about cluster metadata.
	GossipPort int
	// Directory under which this CopyCat instance will put all data it's collecting.
	CopyCatDataDir string
	// Simple address of at least one CopyCat peer to contact. The format is "<machine>:<copycat_port>".
	// If you leave this empty or nil, this node will start a brandnew cluster.
	PeersToContact []string
	// Arbitrary string identifying a geo location. CopyCat will try to distribute data across multiple
	// geo locations to protect data against dependent failures such as power outages, etc.
	// Even though the API doesn't prescribe a string size, shorter is better.
	Location string

	// Popluated internally.
	raftTransport raftTransport
	hostname      string
	logger        *log.Entry
}

// NewCopyCat initiates and starts the copycat framework.
func NewCopyCat(config *Config) (CopyCat, error) {
	return newCopyCat(config)
}

// CopyCat is the struct consumers need to create in order to create distributed data structures.
type CopyCat interface {
	// ConnectToDataStructure allows the consumer to connect to a data structure identified by id.
	// In addition to that a SnapshotProvider needs to be passed in to enable CopyCat to create consistent snapshots.
	// CopyCat responds with a write only proposal channel, a read-only commit channel, a read-only error channel, and
	// a SnapshotConsumer that is used to retrieve consistent snapshots from CopyCat.
	ConnectToDataStructure(id uint64, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer)
	Shutdown()
}

// SnapshotProviders are used to transport a serialized representation
// of a data structure into CopyCat.
type SnapshotProvider func() ([]byte, error)

// SnapshotConsumers are used to transport a serialized representation
// of a data structure out of CopyCat.
type SnapshotConsumer func() ([]byte, error)

// Internal interface.
type store interface {
	raft.Storage
	saveConfigState(confState raftpb.ConfState) error
	saveEntriesAndState(entries []raftpb.Entry, hardState raftpb.HardState) error
	saveSnap(snap raftpb.Snapshot) error
	close()
}

// Internal interface that is only used in CopyCat raft backend.
// It was introduced for mocking purposes.
type raftTransport interface {
	sendMessages(msgs []raftpb.Message)
}

// Internal interface that is only used in CopyCat transport.
// It was introduced for mocking purposes.
type transportRaftBackend interface {
	step(ctx context.Context, msg raftpb.Message) error
	stop()
}

// Internal interface that is only used in CopyCat transport.
// It was introduced for mocking purposes.
type transportMembership interface {
	addDataStructureToRaftIdMapping(dataStructureId uint64, raftId uint64) error
	getAddressForRaftId(raftId uint64) string
	getAddressesForDataStructureId(dataStructureId uint64) []string
}

// Internal interface that is only used in CopyCat.
// It was introduced for mocking purposes.
type copyCatMembership interface {
	addDataStructureToRaftIdMapping(dataStructureId uint64, raftId uint64) error
	getAddr(tags map[string]string) string
	getAllMetadata() map[uint64]map[string]string
	stop() error
}

type locationChooser func(map[string]string) []string
