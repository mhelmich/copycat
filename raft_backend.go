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
	"strconv"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
)

type raftBackend struct {
	raftId    uint64        // cluster-wide unique raft ID
	peers     []pb.Peer     // raft peer URLs
	raftNode  raft.Node     // the actual raft node
	transport raftTransport // the transport used to send raft messages to other backends
	store     store         // the raft data store
	// proposed changes to the data of this raft group
	// write-only for the consumer
	// read-only for the raftBacken
	proposeChan chan []byte
	// proposed changes to the raft group topology
	// write-only for the consumer
	// read-only for the raftBacken
	proposeConfChangeChan chan raftpb.ConfChange
	// changes committed to this raft group
	// write-only for the raftBackend
	// read-only for the consumer
	commitChan chan []byte
	// errors while processing proposed changes
	// write-only for the raftBackend
	// read-only for the consumer
	errorChan chan error
	// this object describes the topology of the raft group this backend is part of
	confState raftpb.ConfState
	// An interactive raft backend returns proposal and commit channels.
	// It listens to them or writes to them respectively.
	// A detached raft backend only answers to messages it receives via the network transport.
	// Backends cannot be converted from one to the other. Once created, they are created.
	isInteractive bool
	// Interactive raft backends have a mechanism to ask the consuming application for the current state
	// of the respective data structure. This is it.
	snapshotProvider SnapshotProvider
	// The last index that has been applied. It helps us figuring out which entries to publish.
	appliedIndex uint64
	// The index of the latest snapshot.
	snapshotIndex uint64
	logger        *log.Entry    // the logger to use by this struct
	stopChan      chan struct{} // signals this raft backend should shut down (only used internally)
}

func newInteractiveRaftBackend(config *Config, peers []pb.Peer, provider SnapshotProvider) (*raftBackend, error) {
	newRaftId := randomRaftId()
	// if there are no peers, I need to at least add myself in order to start a raft group
	peers = append(peers, pb.Peer{
		Id:          newRaftId,
		RaftAddress: config.hostname + ":" + strconv.Itoa(config.CopyCatPort),
	})

	return _newRaftBackend(newRaftId, config, peers, provider, true)
}

func newDetachedRaftBackendWithId(newRaftId uint64, config *Config) (*raftBackend, error) {
	return _newRaftBackend(newRaftId, config, nil, nil, false)
}

func _newRaftBackend(newRaftId uint64, config *Config, peers []pb.Peer, provider SnapshotProvider, isInteractive bool) (*raftBackend, error) {
	logger := config.logger.WithFields(log.Fields{
		"component": "raftBackend",
		"raftId":    uint64ToString(newRaftId),
	})

	storeDir := config.CopyCatDataDir + "raft-" + uint64ToString(newRaftId) + "/"
	startFromExistingState := storageExists(storeDir)
	bs, err := openBoltStorage(storeDir, logger)
	if err != nil {
		config.logger.Errorf("Can't open data store: %s", err.Error())
		return nil, err
	}

	var proposeChan chan []byte
	var proposeConfChangeChan chan raftpb.ConfChange
	var commitChan chan []byte
	var errorChan chan error

	// only create these channels if the backend is interactive
	// otherwise the backend will only listen to messages via the network
	if isInteractive {
		proposeChan = make(chan []byte)
		proposeConfChangeChan = make(chan raftpb.ConfChange)
		commitChan = make(chan []byte)
		errorChan = make(chan error)
	}

	c := &raft.Config{
		ID:              newRaftId,
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         bs,
		MaxSizePerMsg:   1024 * 1024 * 1024, // 1 GB (!!!)
		MaxInflightMsgs: 256,
		Logger:          logger,
	}

	var raftNode raft.Node
	if startFromExistingState {
		hardState, _, _ := bs.InitialState()
		c.Applied = hardState.Commit
		raftNode = raft.RestartNode(c)
	} else {
		rpeers := make([]raft.Peer, len(peers))
		for i := range peers {
			rpeers[i] = raft.Peer{
				ID:      peers[i].Id,
				Context: []byte(peers[i].RaftAddress),
			}
		}

		// peers publishes all known nodes of this raft group
		// raft needs to know how to connect to them
		// note: if rpeers is empty or nil at this point,
		// the new backend will just sit idle and wait to join
		// an existing cluster
		raftNode = raft.StartNode(c, rpeers)
	}

	rb := &raftBackend{
		raftId:                newRaftId,
		peers:                 peers,
		proposeChan:           proposeChan,
		proposeConfChangeChan: proposeConfChangeChan,
		commitChan:            commitChan,
		errorChan:             errorChan,
		store:                 bs,
		raftNode:              raftNode,
		stopChan:              make(chan struct{}),
		transport:             config.raftTransport,
		isInteractive:         isInteractive,
		snapshotProvider:      provider,
		logger:                logger,
	}

	if rb.isInteractive {
		go rb.serveProposalChannels()
	}

	go rb.runRaftStateMachine()
	return rb, nil
}

// this is called from the raft transport server
// every time this raft node receives a message, this code path is invoked
// to kick off the raft state machine
func (rb *raftBackend) step(ctx context.Context, msg raftpb.Message) error {
	return rb.raftNode.Step(ctx, msg)
}

func (rb *raftBackend) serveProposalChannels() {
	var confChangeCount uint64 = 0

	for rb.proposeChan != nil && rb.proposeConfChangeChan != nil {
		select {
		case prop, ok := <-rb.proposeChan:
			if !ok {
				rb.logger.Info("Stopping proposals")
				rb.stop()
				return
			}
			// blocks until accepted by raft state machine
			rb.raftNode.Propose(context.TODO(), prop)

		case cc, ok := <-rb.proposeConfChangeChan:
			if !ok {
				rb.logger.Info("Stopping proposals")
				rb.stop()
				return
			}
			confChangeCount++
			cc.ID = confChangeCount
			rb.raftNode.ProposeConfChange(context.TODO(), cc)
		}
	}
}

func (rb *raftBackend) runRaftStateMachine() {
	snap, err := rb.store.Snapshot()
	if err != nil {
		rb.logger.Fatalf("Can't get base snapshot: %s", err)
		rb.stop()
		return
	}

	rb.confState = snap.Metadata.ConfState
	rb.snapshotIndex = snap.Metadata.Index
	rb.appliedIndex = snap.Metadata.Index
	defer rb.store.close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rb.raftNode.Tick()

		// store raft entries and hard state, then publish changes over commit channel
		case rd := <-rb.raftNode.Ready():
			rb.logger.Debugf("ID: %d Hardstate: %v Entries: %v Snapshot: %v Messages: %v Committed: %v", rb.raftId, rd.HardState, rd.Entries, rd.Snapshot, rd.Messages, rd.CommittedEntries)
			rb.store.saveEntriesAndState(rd.Entries, rd.HardState)

			if !raft.IsEmptySnap(rd.Snapshot) {
				if err := rb.store.saveSnap(rd.Snapshot); err != nil {
					rb.logger.Errorf("Couldn't save snapshot: %s", err.Error())
				}

				// rc.publishSnapshot(rd.Snapshot)
			}

			rb.transport.sendMessages(rd.Messages)
			if ok := rb.publishEntries(rb.entriesToApply(rd.CommittedEntries)); !ok {
				rb.logger.Error("Publishing committed entries failed. Shutting down...")
				rb.stop()
				return
			}
			// rc.maybeTriggerSnapshot()
			rb.raftNode.Advance()

		case <-rb.stopChan:
			rb.logger.Info("Stopping raft state machine loop")
			return
		}
	}
}

func (rb *raftBackend) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	if !rb.isInteractive {
		return
	}

	if len(ents) == 0 {
		return
	}

	firstIdx := ents[0].Index
	if firstIdx > rb.appliedIndex+1 {
		// if I'm getting invalid data, I'm shutting down
		rb.logger.Errorf("first index of committed entry [%d] should <= progress.appliedIndex[%d] !", firstIdx, rb.appliedIndex)
		rb.stop()
	}

	// if I get things that I didn't ask for, I sort them out
	if rb.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rb.appliedIndex-firstIdx+1:]
	}

	return nents
}

func (rb *raftBackend) publishEntries(ents []raftpb.Entry) bool {
	for idx := range ents {
		switch ents[idx].Type {
		case raftpb.EntryNormal:
			if len(ents[idx].Data) > 0 {
				// ignore empty messages
				select {
				case rb.commitChan <- ents[idx].Data:
				case <-rb.stopChan:
					return false
				}
			}

		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[idx].Data)
			rb.logger.Debugf("Publishing config change: [%s]", cc.String())
			rb.confState = *rb.raftNode.ApplyConfChange(cc)
			rb.store.saveConfigState(rb.confState)
			switch cc.Type {
			// TODO: build a raft backend connection cache to the respective peers maybe?
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == rb.raftId {
					rb.logger.Error("I've been removed from the cluster! Shutting down.")
					// this false will the code call stop eventually
					return false
				}
			default:
				rb.logger.Infof("Got the following EntryConfChange event but there's nothing to do: %s", cc.String())
			}
		}
		// after commit, update appliedIndex
		rb.appliedIndex = ents[idx].Index
	}
	return true
}

func (rb *raftBackend) snapshot() ([]byte, error) {
	snap, err := rb.store.Snapshot()
	return snap.Data, err
}

func (rb *raftBackend) stop() {
	rb.stopChan <- struct{}{}
	close(rb.stopChan)
	rb.raftNode.Stop()

	if rb.commitChan != nil {
		close(rb.commitChan)
	}

	if rb.errorChan != nil {
		close(rb.errorChan)
	}
}
