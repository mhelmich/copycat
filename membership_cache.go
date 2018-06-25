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
	newInteractiveRaftBackendForExistingGroupFunc func(config *Config, provider SnapshotProvider) (*raftBackend, error)
	newDetachedRaftBackendWithIdAndPeersFunc      func(newRaftId uint64, config *Config, peers []*pb.RaftPeer) (*raftBackend, error)
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
		newInteractiveRaftBackendForExistingGroupFunc: newInteractiveRaftBackendForExistingGroup,
		newDetachedRaftBackendWithIdAndPeersFunc:      newDetachedRaftBackendWithIdAndPeers,
		newRaftTransportServiceClientFunc:             pb.NewRaftTransportServiceClient,
		newCopyCatServiceClientFunc:                   pb.NewCopyCatServiceClient,
		connectionCacheFunc:                           _getConnectionForAddress,
		logger:                                        config.logger.WithFields(log.Fields{}),
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

func (mc *membershipCache) peersForDataStructureId(dataStructureId ID) []*pb.RaftPeer {
	return mc.membership.peersForDataStructureId(dataStructureId)
}

func (mc *membershipCache) onePeerForDataStructureId(dataStructureId ID) (*pb.RaftPeer, error) {
	return mc.membership.onePeerForDataStructureId(dataStructureId)
}

// called by copyCatTransport
func (mc *membershipCache) newDetachedRaftBackend(dataStructureId ID, raftId uint64, config *Config, peers []*pb.RaftPeer) (*raftBackend, error) {
	backend, err := mc.newDetachedRaftBackendWithIdAndPeersFunc(raftId, config, peers)
	if err != nil {
		return nil, err
	}

	err = mc.publishMetadataAboutRaftBackend(dataStructureId, backend)
	return backend, err
}

func (mc *membershipCache) newInteractiveRaftBackendForExistingGroup(dataStructureId ID, config *Config, provider SnapshotProvider) (*raftBackend, error) {
	backend, err := mc.newInteractiveRaftBackendForExistingGroupFunc(config, provider)
	if err != nil {
		return nil, err
	}

	err = mc.publishMetadataAboutRaftBackend(dataStructureId, backend)
	return backend, err
}

func (mc *membershipCache) publishMetadataAboutRaftBackend(dataStructureId ID, backend *raftBackend) error {
	_, loaded := mc.raftIdToRaftBackend.LoadOrStore(backend.raftId, backend)
	if loaded {
		defer backend.stop()
		return fmt.Errorf("Raft backend with id [%d %x] existed already", backend.raftId, backend.raftId)
	}

	mc.logger.Errorf("Publish metadata %s %d %x", dataStructureId.String(), backend.raftId, backend.raftId)
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

func (mc *membershipCache) startNewRaftGroup(dataStructureId ID, numReplicas int) ([]*pb.RaftPeer, error) {
	pickedPeerIds, err := mc.startRaftsRemotely(dataStructureId, numReplicas)
	// all for 3 retries (4 in total)
	for i := 0; i < 3 && err != nil; i++ {
		mc.logger.Errorf("Can't start new raft group: %s", err.Error())
		pickedPeerIds, err = mc.startRaftsRemotely(dataStructureId, numReplicas)
	}

	if err != nil {
		return nil, err
	}

	return pickedPeerIds, nil
}

func (mc *membershipCache) startRaftsRemotely(dataStructureId ID, numReplicas int) ([]*pb.RaftPeer, error) {
	pickedPeerIds := mc.membership.pickReplicaPeers(dataStructureId, numReplicas)
	if pickedPeerIds == nil || len(pickedPeerIds) == 0 {
		return nil, fmt.Errorf("Picking replica peers failed")
	}

	raftPeers := make([]*pb.RaftPeer, len(pickedPeerIds))
	for idx, peerId := range pickedPeerIds {
		raftPeers[idx] = &pb.RaftPeer{
			RaftId:      randomRaftId(),
			PeerAddress: mc.membership.getAddressForPeer(peerId),
		}
	}

	peerCh := make(chan *pb.RaftPeer)
	ctx, cancelFunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFunc()
	for _, peer := range raftPeers {
		go mc.startRaftRemotely(ctx, peerCh, dataStructureId, peer, raftPeers)
	}

	for j := 0; j < len(raftPeers); j++ {
		select {
		case peer, ok := <-peerCh:
			if !ok {
				return nil, errCantFindEnoughReplicas
			}

			if peer == nil {
				// starting one of the rafts failed!
				// in that case we pack our bags and go home
				mc.stopAllRaftsAsync(pickedPeerIds)
				return nil, errCantFindEnoughReplicas
			}

			mc.logger.Infof("Started raft [%d %x] for data structure [%s]", peer.RaftId, peer.RaftId, dataStructureId.String())
		case <-ctx.Done():
			// if the time's up, we return an error and stop all rafts
			mc.stopAllRaftsAsync(pickedPeerIds)
			return nil, errCantFindEnoughReplicas
		}
	}

	return raftPeers, nil
}

func (mc *membershipCache) stopAllRaftsAsync(peerIds []uint64) {
	for _, peerId := range peerIds {
		addr := mc.membership.getAddressForPeer(peerId)
		defer mc.stopRaftRemotely(pb.RaftPeer{
			RaftId:      peerId,
			PeerAddress: addr,
		})
	}
}

func (mc *membershipCache) addRaftToGroupRemotely(newRaftId uint64, peer *pb.RaftPeer) error {
	client, err := mc.newCopyCatServiceClient(mc.addressToConnection, peer.PeerAddress)
	if err != nil {
		err = fmt.Errorf("Can't connect to %d %x %s: %s", peer.RaftId, peer.RaftId, peer.PeerAddress, err.Error())
		mc.logger.Errorf("%s", err.Error())
		return err
	}

	_, err = client.AddRaftToRaftGroup(context.TODO(), &pb.AddRaftRequest{
		NewRaftId:      newRaftId,
		ExistingRaftId: peer.RaftId,
	})
	if err != nil {
		err = fmt.Errorf("Can't add raft to raft group: %s", err.Error())
		mc.logger.Errorf("%s", err.Error())
		return err
	}
	return nil
}

func (mc *membershipCache) startRaftRemotely(ctx context.Context, peerCh chan *pb.RaftPeer, dataStructureId ID, raftPeer *pb.RaftPeer, allPeers []*pb.RaftPeer) {
	if raftPeer.PeerAddress == "" {
		// signal to the listener that we need to retry another peer
		peerCh <- nil
		return
	}

	client, err := mc.newCopyCatServiceClient(mc.addressToConnection, raftPeer.PeerAddress)
	if err != nil {
		mc.logger.Errorf("Can't connect to %s: %s", raftPeer.PeerAddress, err.Error())
		peerCh <- nil
		return
	}

	resp, err := client.StartRaft(ctx, &pb.StartRaftRequest{
		DataStructureId: dataStructureId.toProto(),
		RaftIdToUse:     raftPeer.RaftId,
		AllRaftPeers:    allPeers,
	})
	if err != nil {
		mc.logger.Errorf("Can't start a raft at %s: %s", raftPeer.PeerAddress, err.Error())
		// signal to the listener that we need to retry another peer
		peerCh <- nil
		return
	}

	mc.logger.Infof("Started raft remotely on %s: %s", raftPeer.PeerAddress, resp.String())
	peerCh <- &pb.RaftPeer{
		RaftId:      resp.RaftId,
		PeerAddress: resp.RaftAddress,
	}
}

func (mc *membershipCache) stopRaftRemotely(peer pb.RaftPeer) error {
	if peer.PeerAddress == "" {
		return fmt.Errorf("Invalid address: %s", peer.PeerAddress)
	}

	client, err := mc.newCopyCatServiceClient(mc.addressToConnection, peer.PeerAddress)
	if err != nil {
		return err
	}

	_, err = client.StopRaft(context.TODO(), &pb.StopRaftRequest{RaftId: peer.RaftId})
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
		// TODO: maybe gossip around to ask for a particular raft?
		//       or even for the entire data structure?
		err := fmt.Errorf("Can't find raft with id [%d %x] because can't find network address", raftId, raftId)
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
