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

type membershipCache struct {
	membership                                    memberList
	raftIdToRaftBackend                           *sync.Map
	addressToConnection                           *sync.Map
	chooserFunc                                   func(peerId uint64, tags map[string]string) bool
	newInteractiveRaftBackendFunc                 func(config *Config, peers []pb.Peer, provider SnapshotProvider) (*raftBackend, error)
	newInteractiveRaftBackendForExistingGroupFunc func(config *Config, provider SnapshotProvider) (*raftBackend, error)
	newDetachedRaftBackendWithIdFunc              func(newRaftId uint64, config *Config) (*raftBackend, error)
	newRaftTransportServiceClientFunc             func(conn *grpc.ClientConn) pb.RaftTransportServiceClient
	newCopyCatServiceClientFunc                   func(conn *grpc.ClientConn) pb.CopyCatServiceClient
	connectionCacheFunc                           func(m *sync.Map, address string) (*grpc.ClientConn, error)
	myAddress                                     string
	logger                                        *log.Entry
}

func newMembershipCache(config *Config) (*membershipCache, error) {
	m, err := newMembership(config)
	if err != nil {
		config.logger.Errorf("Can't create membership cache: %s", err.Error())
		return nil, err
	}

	return &membershipCache{
		membership:          m,
		myAddress:           config.address(),
		raftIdToRaftBackend: &sync.Map{},
		addressToConnection: &sync.Map{},
		// Yet another level of indirection used for unit testing
		newInteractiveRaftBackendFunc:                 _newInteractiveRaftBackend,
		newInteractiveRaftBackendForExistingGroupFunc: _newInteractiveRaftBackendForExistingGroup,
		newDetachedRaftBackendWithIdFunc:              _newDetachedRaftBackendWithId,
		newRaftTransportServiceClientFunc:             _newRaftTransportServiceClient,
		newCopyCatServiceClientFunc:                   _newCopyCatServiceClient,
		connectionCacheFunc:                           _getConnectionForAddress,
		chooserFunc: func(peerId uint64, tags map[string]string) bool {
			return true
		},
		logger: config.logger.WithFields(log.Fields{}),
	}, nil
}

func (mc *membershipCache) stepRaft(ctx context.Context, msg raftpb.Message) error {
	val, ok := mc.raftIdToRaftBackend.Load(msg.To)
	if !ok {
		return fmt.Errorf("Can't find raft backend with id: %d %x", msg.To, msg.To)
	}

	backend := val.(*raftBackend)
	// invoke the raft state machine
	return backend.step(ctx, msg)
}

func (mc *membershipCache) addToRaftGroup(ctx context.Context, existingRaftId uint64, newRaftId uint64) error {
	val, ok := mc.raftIdToRaftBackend.Load(existingRaftId)
	if !ok {
		return fmt.Errorf("Can't find raft backend with id: %d %x", existingRaftId, existingRaftId)
	}

	backend := val.(*raftBackend)
	// invokes propose config change under the covers
	err := backend.addRaftToMyGroup(ctx, newRaftId)
	if err != nil {
		return fmt.Errorf("Can't add raft [%d %x] to raft group of [%d %x]: %s", newRaftId, newRaftId, existingRaftId, existingRaftId, err.Error())
	}

	return nil
}

func (mc *membershipCache) peersForDataStructureId(dataStructureId uint64) []pb.Peer {
	return mc.membership.peersForDataStructureId(dataStructureId)
}

func (mc *membershipCache) onePeerForDataStructureId(dataStructureId uint64) (*pb.Peer, error) {
	return mc.membership.onePeerForDataStructureId(dataStructureId)
}

func (mc *membershipCache) newDetachedRaftBackend(dataStructureId uint64, raftId uint64, config *Config) (*raftBackend, error) {
	backend, err := mc.newDetachedRaftBackendWithIdFunc(raftId, config)
	if err != nil {
		return nil, err
	}

	err = mc.stashRaftBackend(dataStructureId, backend)
	return backend, err
}

func (mc *membershipCache) newInteractiveRaftBackend(dataStructureId uint64, config *Config, peers []pb.Peer, provider SnapshotProvider) (*raftBackend, error) {
	backend, err := mc.newInteractiveRaftBackendFunc(config, peers, provider)
	if err != nil {
		return nil, err
	}

	err = mc.stashRaftBackend(dataStructureId, backend)
	return backend, err
}

func (mc *membershipCache) newInteractiveRaftBackendForExistingGroup(dataStructureId uint64, config *Config, provider SnapshotProvider) (*raftBackend, error) {
	backend, err := mc.newInteractiveRaftBackendForExistingGroupFunc(config, provider)
	if err != nil {
		return nil, err
	}

	err = mc.stashRaftBackend(dataStructureId, backend)
	return backend, err
}

func (mc *membershipCache) stashRaftBackend(dataStructureId uint64, backend *raftBackend) error {
	_, loaded := mc.raftIdToRaftBackend.LoadOrStore(backend.raftId, backend)
	if loaded {
		defer backend.stop()
		return fmt.Errorf("Raft backend with id [%d %x] existed already", backend.raftId, backend.raftId)
	}

	return mc.membership.addDataStructureToRaftIdMapping(dataStructureId, backend.raftId)
}

func (mc *membershipCache) stopRaft(raftId uint64) error {
	val, ok := mc.raftIdToRaftBackend.Load(raftId)
	if ok {
		backend := val.(*raftBackend)
		defer backend.stop()
	}
	mc.raftIdToRaftBackend.Delete(raftId)
	return mc.membership.removeDataStructureToRaftIdMapping(raftId)
}

func (mc *membershipCache) getRaftTransportClientForRaftId(raftId uint64) (pb.RaftTransportServiceClient, error) {
	addr := mc.membership.getAddressForRaftId(raftId)
	if addr == "" {
		return nil, fmt.Errorf("Can't find raft with id: %d %x", raftId, raftId)
	}

	return mc.newRaftTransportServiceClient(mc.addressToConnection, addr)
}

func (mc *membershipCache) chooseReplicaNode(dataStructureId uint64, numReplicas int) ([]pb.Peer, error) {
	notMePicker := func(peerId uint64, tags map[string]string) bool {
		return peerId != mc.membership.myGossipNodeId() && mc.chooserFunc(peerId, tags)
	}

	pickedPeerIds := mc.membership.pickFromMetadata(notMePicker, numReplicas, make([]uint64, 0))
	peerCh := make(chan *pb.Peer)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	for _, peerId := range pickedPeerIds {
		addr := mc.membership.getAddressForPeer(peerId)
		go mc.startRaftRemotely(ctx, peerCh, dataStructureId, addr)
	}

	newRafts := make([]pb.Peer, numReplicas)
	j := 0

	for {
		select {
		case peer, ok := <-peerCh:
			if !ok {
				return newRafts[:j], nil
			}

			if peer == nil {
				// I got an empty response from one of the peers I contacted,
				// let me try another one...
				newPickedPeerIds := mc.membership.pickFromMetadata(notMePicker, 1, pickedPeerIds)
				for _, peerId := range newPickedPeerIds {
					addr := mc.membership.getAddressForPeer(peerId)
					go mc.startRaftRemotely(ctx, peerCh, dataStructureId, addr)
					//avoid making the same mistake twice
					pickedPeerIds = append(pickedPeerIds, peerId)
				}
			} else {

				// if I got a valid response, put it in the array
				newRafts[j] = *peer
				j++
				if j >= numReplicas {
					return newRafts, nil
				}

			}
		case <-ctx.Done():
			return newRafts[:j], errCantFindEnoughReplicas
		}
	}
}

func (mc *membershipCache) addRaftToGroupRemotely(newRaftId uint64, peer *pb.Peer) error {
	client, err := mc.newCopyCatServiceClient(mc.addressToConnection, peer.RaftAddress)
	if err != nil {
		err = fmt.Errorf("Can't connect to %d %x %s: %s", peer.Id, peer.Id, peer.RaftAddress, err.Error())
		mc.logger.Errorf("%s", err.Error())
		return err
	}

	req := &pb.AddRaftRequest{
		NewRaftId:      newRaftId,
		ExistingRaftId: peer.Id,
	}
	_, err = client.AddRaftToRaftGroup(context.TODO(), req)
	if err != nil {
		err = fmt.Errorf("Can't add raft to raft group: %s", err.Error())
		mc.logger.Errorf("%s", err.Error())
		return err
	}
	return nil
}

func (mc *membershipCache) startRaftRemotely(ctx context.Context, peerCh chan *pb.Peer, dataStructureId uint64, address string) {
	if mc.myAddress == address || address == "" {
		// signal to the listener that we need to retry another peer
		peerCh <- nil
		return
	}

	client, err := mc.newCopyCatServiceClient(mc.addressToConnection, address)
	if err != nil {
		mc.logger.Errorf("Can't connect to %s: %s", address, err.Error())
		peerCh <- nil
		return
	}

	resp, err := client.StartRaft(ctx, &pb.StartRaftRequest{DataStructureId: dataStructureId})
	if err != nil {
		mc.logger.Errorf("Can't start a raft at %s: %s", address, err.Error())
		// signal to the listener that we need to retry another peer
		peerCh <- nil
		return
	}

	mc.logger.Infof("Started raft remotely on %s: %s", address, resp.String())
	peerCh <- &pb.Peer{
		Id:          resp.RaftId,
		RaftAddress: resp.RaftAddress,
	}
}

func (mc *membershipCache) stopRaftRemotely(peer pb.Peer) error {
	if peer.RaftAddress == "" {
		return fmt.Errorf("Invalid address: %s", peer.RaftAddress)
	}

	client, err := mc.newCopyCatServiceClient(mc.addressToConnection, peer.RaftAddress)
	if err != nil {
		return err
	}

	_, err = client.StopRaft(context.TODO(), &pb.StopRaftRequest{RaftId: peer.Id})
	return err
}

func (mc *membershipCache) newRaftTransportServiceClient(m *sync.Map, address string) (pb.RaftTransportServiceClient, error) {
	conn, err := mc.connectionCacheFunc(m, address)
	if err != nil {
		return nil, err
	}

	return mc.newRaftTransportServiceClientFunc(conn), nil
}

func (mc *membershipCache) getRaftTransportServiceClientForRaftId(raftId uint64) (pb.RaftTransportServiceClient, error) {
	addr := mc.membership.getAddressForRaftId(raftId)
	if addr == "" {
		err := fmt.Errorf("Can't find raft with id [%d %x] because addr was empty", raftId, raftId)
		mc.logger.Errorf("%s", err.Error())
		return nil, err
	}
	return mc.newRaftTransportServiceClient(mc.addressToConnection, addr)
}

func (mc *membershipCache) newCopyCatServiceClient(m *sync.Map, address string) (pb.CopyCatServiceClient, error) {
	conn, err := mc.connectionCacheFunc(m, address)
	if err != nil {
		return nil, err
	}

	return mc.newCopyCatServiceClientFunc(conn), nil
}

// This is a best-effort stop.
// Other go routines could come in and create new backends or connections
// at the same time.
func (mc *membershipCache) stop() error {
	mc.membership.stop()

	mc.raftIdToRaftBackend.Range(func(key, value interface{}) bool {
		backend := value.(*raftBackend)
		defer backend.stop()
		return true
	})

	mc.addressToConnection.Range(func(key, value interface{}) bool {
		conn := value.(*grpc.ClientConn)
		defer conn.Close()
		return true
	})

	return nil
}

func _newInteractiveRaftBackend(config *Config, peers []pb.Peer, provider SnapshotProvider) (*raftBackend, error) {
	return newInteractiveRaftBackend(config, peers, provider)
}

func _newInteractiveRaftBackendForExistingGroup(config *Config, provider SnapshotProvider) (*raftBackend, error) {
	return newInteractiveRaftBackendForExistingGroup(config, provider)
}

func _newDetachedRaftBackendWithId(newRaftId uint64, config *Config) (*raftBackend, error) {
	return newDetachedRaftBackendWithId(newRaftId, config)
}

func _newRaftTransportServiceClient(conn *grpc.ClientConn) pb.RaftTransportServiceClient {
	return pb.NewRaftTransportServiceClient(conn)
}

func _newCopyCatServiceClient(conn *grpc.ClientConn) pb.CopyCatServiceClient {
	return pb.NewCopyCatServiceClient(conn)
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
