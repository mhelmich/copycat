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
	"fmt"
	"sync"
	"time"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

type raftBackendCache struct {
	membership          *membership
	raftIdToRaftBackend *sync.Map
	addressToConnection *sync.Map
	chooser             func(peerId uint64, tags map[string]string) bool
	connectionCacheFunc func(m *sync.Map, address string) (*grpc.ClientConn, error)
	myAddress           string
	logger              *log.Entry
}

func newRaftBackendCache(m *membership) *raftBackendCache {
	return &raftBackendCache{
		membership:          m,
		raftIdToRaftBackend: &sync.Map{},
		addressToConnection: &sync.Map{},
		connectionCacheFunc: _getConnectionForAddress,
		chooser: func(peerId uint64, tags map[string]string) bool {
			return true
		},
	}
}

func (rbc *raftBackendCache) stepRaft(ctx context.Context, msg raftpb.Message) error {
	val, ok := rbc.raftIdToRaftBackend.Load(msg.To)
	if !ok {
		return fmt.Errorf("Can't find raft backend with id: %d", msg.To)
	}

	backend := val.(transportRaftBackend)
	// invoke the raft state machine
	return backend.step(ctx, msg)
}

func (rbc *raftBackendCache) newDetachedRaftBackend(raftId uint64, config *Config) (transportRaftBackend, error) {
	backend, err := newDetachedRaftBackendWithId(raftId, config)
	if err == nil {
		_, loaded := rbc.raftIdToRaftBackend.LoadOrStore(raftId, backend)
		if loaded {
			defer backend.stop()
			return nil, fmt.Errorf("Raft backend with id [%d] existed already", raftId)
		}
	}
	return backend, err
}

func (rbc *raftBackendCache) newInteractiveRaftBackend(config *Config, peers []pb.Peer, provider SnapshotProvider) (*raftBackend, error) {
	backend, err := newInteractiveRaftBackend(config, peers, provider)
	if err == nil {
		_, loaded := rbc.raftIdToRaftBackend.LoadOrStore(backend.raftId, backend)
		if loaded {
			defer backend.stop()
			return nil, fmt.Errorf("Raft backend with id [%d] existed already", backend.raftId)
		}
	}
	return backend, err
}

func (rbc *raftBackendCache) getRaftTransportClientForRaftId(raftId uint64) (pb.RaftTransportServiceClient, error) {
	addr := rbc.membership.getAddressForRaftId(raftId)
	if addr == "" {
		return nil, fmt.Errorf("Can't find raft with id: %d", raftId)
	}

	return rbc.newRaftTransportServiceClient(rbc.addressToConnection, addr)
}

func (rbc *raftBackendCache) chooseReplicaNode(dataStructureId uint64, numReplicas int) ([]pb.Peer, error) {
	pickedPeerIds := rbc.membership.pickFromMetadata(rbc.chooser, 3)
	peerCh := make(chan *pb.Peer)
	for _, peerId := range pickedPeerIds {
		addr := rbc.membership.getAddressForPeer(peerId)
		go rbc.startRaftRemotely(peerCh, dataStructureId, addr)
	}

	newRafts := make([]pb.Peer, numReplicas)
	timeout := time.Now().Add(5 * time.Second)
	j := 0

	for {
		select {
		case peer, ok := <-peerCh:
			if !ok {
				return newRafts, nil
			}

			if peer == nil {
				// TODO
				// I got an empty response from one of the peers I contacted,
				// let me try another one...
			} else {

				// if I got a valid response, put it in the array
				newRafts[j] = *peer
				j++
				if j >= numReplicas {
					return newRafts, nil
				}

			}
		case <-time.After(time.Until(timeout)):
			return newRafts, fmt.Errorf("Couldn't find enough remote peers for raft creation and timed out")
		}
	}
}

func (rbc *raftBackendCache) startRaftRemotely(peerCh chan *pb.Peer, dataStructureId uint64, address string) {
	if rbc.myAddress == address {
		// signal to the listener that we need to retry another peer
		peerCh <- nil
		return
	}

	client, err := rbc.newCopyCatServiceClient(rbc.addressToConnection, address)
	if err != nil {
		rbc.logger.Errorf("Can't connect to %s: %s", address, err.Error())
		peerCh <- nil
		return
	}

	resp, err := client.StartRaft(context.TODO(), &pb.StartRaftRequest{DataStructureId: dataStructureId})
	if err != nil {
		rbc.logger.Errorf("Can't start a raft at %s: %s", address, err.Error())
		// signal to the listener that we need to retry another peer
		peerCh <- nil
		return
	}

	rbc.logger.Infof("Started raft remotely on %s: %s", address, resp.String())
	peerCh <- &pb.Peer{
		Id:          resp.RaftId,
		RaftAddress: resp.RaftAddress,
	}
}

func (rbc *raftBackendCache) stopRaftRemotely(peer pb.Peer) error {
	if peer.RaftAddress == "" {
		return nil
	}

	client, err := rbc.newCopyCatServiceClient(rbc.addressToConnection, peer.RaftAddress)
	if err != nil {
		return err
	}

	_, err = client.StopRaft(context.TODO(), &pb.StopRaftRequest{RaftId: peer.Id})
	return err
}

func (rbc *raftBackendCache) newRaftTransportServiceClient(m *sync.Map, address string) (pb.RaftTransportServiceClient, error) {
	conn, err := rbc.connectionCacheFunc(m, address)
	if err != nil {
		return nil, err
	}

	return pb.NewRaftTransportServiceClient(conn), nil
}

func (rbc *raftBackendCache) newCopyCatServiceClient(m *sync.Map, address string) (pb.CopyCatServiceClient, error) {
	conn, err := rbc.connectionCacheFunc(m, address)
	if err != nil {
		return nil, err
	}

	return pb.NewCopyCatServiceClient(conn), nil
}

// This is a best-effort stop.
// Other go routines could come in and create new backends or connections
// at the same time.
func (rbc *raftBackendCache) stop() {
	rbc.raftIdToRaftBackend.Range(func(key, value interface{}) bool {
		backend := value.(*raftBackend)
		defer backend.stop()
		return true
	})

	rbc.addressToConnection.Range(func(key, value interface{}) bool {
		conn := value.(*grpc.ClientConn)
		defer conn.Close()
		return true
	})
}

func _getConnectionForAddress(m *sync.Map, address string) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error
	var val interface{}
	var ok bool
	var loaded bool

	val, ok = m.Load(address)
	if !ok {
		conn, err = grpc.Dial(address, grpc.WithInsecure())
		if err != nil {
			return nil, err
		}

		val, loaded = m.LoadOrStore(address, conn)
		if loaded {
			// somebody else stored the connection already
			// all of this was for nothing
			defer conn.Close()
		}
	}

	conn = val.(*grpc.ClientConn)
	return conn, nil
}
