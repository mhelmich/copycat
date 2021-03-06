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
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
)

const (
	defaultCopyCatPort    = 11983
	defaultCopyCatDataDir = "./copycat.db/"
)

func DefaultConfig() *Config {
	host, _ := os.Hostname()
	return &Config{
		Hostname:       host,
		CopyCatPort:    defaultCopyCatPort,
		GossipPort:     defaultCopyCatPort + 1000,
		CopyCatDataDir: defaultCopyCatDataDir,
	}
}

// Config is the public configuration struct that needs to be passed into the constructor function.
type Config struct {
	// Advertized host name
	Hostname string
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
	// A data center is such an arbitrary geo location.
	DataCenter string
	// A rack is a section within a data center. Even though you might not know which rack your nodes live in,
	// this can be used to create a second hierarchy for data placement.
	Rack string

	// Populated internally.
	raftTransport raftTransport
	logger        *log.Entry
}

func (c *Config) String() string {
	return fmt.Sprintf("CopyCatConfig: Hostname: [%s] CopyCatPort: [%d] GossipPort: [%d] DataCenter: [%s] Rack: [%s] Peers: [%s] DataDir: [%s]", c.Hostname, c.CopyCatPort, c.GossipPort, c.DataCenter, c.Rack, strings.Join(c.PeersToContact, ", "), c.CopyCatDataDir)
}

func (c *Config) address() string {
	return c.Hostname + ":" + strconv.Itoa(c.CopyCatPort)
}

// NewCopyCat initiates and starts the copycat framework.
func NewCopyCat(config *Config) (CopyCat, error) {
	return newCopyCat(config)
}

// this sstruct captures the options
type allocationOptions struct {
	dataCenterReplicas int
}

func defaultAllocationOptions() *allocationOptions {
	return &allocationOptions{
		dataCenterReplicas: 1,
	}
}

type AllocationOption func(*allocationOptions)

func WithDataCenterReplicas(numDataCenterReplicas int) AllocationOption {
	return func(allocOpts *allocationOptions) {
		allocOpts.dataCenterReplicas = numDataCenterReplicas
	}
}

// CopyCat is the struct consumers need to create in order to create distributed data structures.
type CopyCat interface {
	// Acquires a new CopyCat data structure id. The consumer is in the responsibility to keep this id save.
	// If the id is lost, the data structure can't be accessed anymore.
	NewDataStructureID() (*ID, error)
	// AllocateNewDataStructure creates a new data structure and returns its unique ID.
	// The consumer is in the responsibility to keep this id save. If the id is lost, the data structure can't be accessed anymore.
	// TODO - change the API to return the id right away
	// and a channel that is closed when the raft is being created
	// this way consumers can move on with storing the ID somewhere while they wait for their raft groups to come up
	AllocateNewDataStructure(opts ...AllocationOption) (*ID, error)
	// Convenience wrapper to connect to a data structure with the string representation of an id.
	SubscribeToDataStructureWithStringID(id string, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer, error)
	// SubscribeToDataStructure allows the consumer to connect to a data structure identified by id.
	// In addition to that a SnapshotProvider needs to be passed in to enable CopyCat to create consistent snapshots.
	// CopyCat responds with a write only proposal channel, a read-only commit channel, a read-only error channel, and
	// a SnapshotConsumer that is used to retrieve consistent snapshots from CopyCat.
	SubscribeToDataStructure(id *ID, provider SnapshotProvider) (chan<- []byte, <-chan []byte, <-chan error, SnapshotConsumer, error)
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
	dropLogEntriesBeforeIndex(index uint64) error
	saveSnap(snap raftpb.Snapshot) error
	dropOldSnapshots(numberOfSnapshotsToKeep int) error
	close()
}

// Internal interface that is only used in CopyCat raft backend.
// It was introduced for mocking purposes.
type raftTransport interface {
	sendMessages(msgs []raftpb.Message) *messageSendingResults
}

// Internal interface that is used in copycat and the transport.
// It was introduced for mocking purposes.
type membershipProxy interface {
	// called by copyCatTransport
	getRaftTransportServiceClientForRaftId(raftId uint64) (pb.RaftTransportServiceClient, error)
	// this call needs to be idempotent as it is called multiple times for the same input
	newDetachedRaftBackend(dataStructureId *ID, raftId uint64, config *Config, peers []*pb.RaftPeer) (*raftBackend, error)
	stepRaft(ctx context.Context, msg raftpb.Message) error
	addLearnerToRaftGroup(ctx context.Context, existingRaftId uint64, newRaftId uint64) error

	// called by CopyCat
	onePeerForDataStructureId(dataStructureId *ID) (*pb.RaftPeer, error)
	startNewRaftGroup(dataStructureId *ID, numReplicas int) ([]*pb.RaftPeer, error)
	newInteractiveRaftBackendForExistingGroup(dataStructureId *ID, config *Config, provider SnapshotProvider) (*raftBackend, error)
	stopRaft(raftId uint64) error
	addLearnerToRaftGroupRemotely(newRaftId uint64, peers *pb.RaftPeer) error
	stop() error
}

// Internal interface that is used in copycat and the transport.
// It was introduced for mocking purposes.
type memberList interface {
	onePeerForDataStructureId(dataStructureId *ID) (*pb.RaftPeer, error)
	peersForDataStructureId(dataStructureId *ID) []*pb.RaftPeer
	addDataStructureToRaftIdMapping(dataStructureId *ID, raftId uint64) error
	removeDataStructureToRaftIdMapping(raftId uint64) error
	getAddressForRaftId(raftId uint64) string
	pickReplicaPeers(dataStructureId *ID, numReplicas int) []uint64
	getAddressForPeer(peerId uint64) string
	getNumReplicasForDataStructure(id *ID) int
	stop() error
}

////////////////////////
///////// errors

// ErrCantFindEnoughReplicas is returned of the number of replicas
// for an allocation request can't be fulfilled
var ErrCantFindEnoughReplicas = errors.New("Couldn't find enough remote peers for raft creation and timed out")
