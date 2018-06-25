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
	"encoding/hex"
	"sync/atomic"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
)

type raftBackend struct {
	raftId    uint64         // cluster-wide unique raft ID
	peers     []*pb.RaftPeer // raft peer URLs
	raftNode  raft.Node      // the actual raft node
	transport raftTransport  // the transport used to send raft messages to other backends
	store     store          // the raft data store
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
	// Keeps track of the latest config change id. The next id is this id + 1.
	// HACK - there's obviously a race condition around this considering that
	// whatever id this raft node has, the leader in the group might have a higher one
	latestConfChangeId uint64
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
	// The index of the latest snapshot. Used to compute when to cut the next snapshot.
	snapshotIndex uint64
	// The number of log entries after which we cut a snapshot.
	snapshotFrequency uint64
	// Defines the number of snapshots that CopyCat keeps before deleting old ones.
	numberOfSnapshotsToKeep int
	logger                  *log.Entry    // the logger to use by this struct
	stopChan                chan struct{} // signals this raft backend should shut down (only used internally)
	alreadyClosed           int32         // atomic boolean indicating whether this raft has been closed or not
}

func newInteractiveRaftBackendForExistingGroup(config *Config, provider SnapshotProvider) (*raftBackend, error) {
	return _newRaftBackend(randomRaftId(), config, nil, provider, true)
}

func newDetachedRaftBackendWithIdAndPeers(newRaftId uint64, config *Config, peers []*pb.RaftPeer) (*raftBackend, error) {
	return _newRaftBackend(newRaftId, config, peers, nil, false)
}

func _newRaftBackend(newRaftId uint64, config *Config, peers []*pb.RaftPeer, provider SnapshotProvider, isInteractive bool) (*raftBackend, error) {
	logger := config.logger.WithFields(log.Fields{
		"component": "raftBackend",
		"raftIdHex": hex.EncodeToString(uint64ToBytes(newRaftId)),
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
		raftPeers := make([]raft.Peer, len(peers))
		for i := range peers {
			raftPeers[i] = raft.Peer{
				ID:      peers[i].RaftId,
				Context: []byte(peers[i].PeerAddress),
			}
		}

		// peers publishes all known nodes of this raft group
		// raft needs to know how to connect to them
		// note: if rpeers is empty or nil at this point,
		// the new backend will just sit idle and wait to join
		// an existing cluster
		raftNode = raft.StartNode(c, raftPeers)
	}

	rb := &raftBackend{
		raftId:                  newRaftId,
		peers:                   peers,
		proposeChan:             proposeChan,
		proposeConfChangeChan:   proposeConfChangeChan,
		commitChan:              commitChan,
		errorChan:               errorChan,
		store:                   bs,
		raftNode:                raftNode,
		stopChan:                make(chan struct{}),
		transport:               config.raftTransport,
		isInteractive:           isInteractive,
		snapshotProvider:        provider,
		snapshotFrequency:       uint64(1000),
		numberOfSnapshotsToKeep: 2,
		latestConfChangeId:      uint64(0),
		logger:                  logger,
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
	for rb.proposeChan != nil && rb.proposeConfChangeChan != nil {
		select {
		case prop, ok := <-rb.proposeChan:
			if !ok {
				rb.logger.Info("Stopping proposals")
				rb.stop()
				return
			}
			// blocks until accepted by raft state machine
			err := rb.raftNode.Propose(context.TODO(), prop)
			if err != nil {
				rb.logger.Errorf("Failed to propose change to raft: %s", err.Error())
			}

		case cc, ok := <-rb.proposeConfChangeChan:
			if !ok {
				rb.logger.Info("Stopping proposals")
				rb.stop()
				return
			}
			// TODO: there is a data race here!!!
			cc.ID = rb.latestConfChangeId + 1
			err := rb.raftNode.ProposeConfChange(context.TODO(), cc)
			if err != nil {
				rb.logger.Errorf("Failed to propose change to raft: %s", err.Error())
			}

		case <-rb.stopChan:
			return
		}
	}
}

func (rb *raftBackend) runRaftStateMachine() {
	snap, err := rb.store.Snapshot()
	if err != nil {
		rb.logger.Errorf("Can't get base snapshot: %s", err)
		rb.stop()
		return
	}

	rb.confState = snap.Metadata.ConfState
	rb.snapshotIndex = snap.Metadata.Index
	rb.appliedIndex = snap.Metadata.Index
	// If the snapshot is not empty, that means we need to provoke the consumer.
	// Then reload its current state from this snapshot by sending nil to the commit channel.
	if rb.isInteractive && !raft.IsEmptySnap(snap) {
		select {
		case rb.commitChan <- nil:
		case <-rb.stopChan:
			return
		}
	}
	defer rb.store.close()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// event loop over raft state machine updates
	for {
		select {
		case <-ticker.C:
			rb.raftNode.Tick()

		case rd := <-rb.raftNode.Ready():
			if !rb.procesReady(rd) {
				rb.logger.Error("Publishing committed entries failed. Shutting down...")
				rb._stop(true)
				return
			}

		case <-rb.stopChan:
			rb.logger.Info("Stopping raft state machine loop")
			rb._stop(false)
			return
		}
	}
}

// Store raft entries and hard state, then publish changes over commit channel.
// Returning false will stop the raft state machine!
func (rb *raftBackend) procesReady(rd raft.Ready) bool {
	rb.logger.Debugf("ID: %d %x Hardstate: %v Entries: %v Snapshot: %v Messages: %v Committed: %v ConfState: %s", rb.raftId, rb.raftId, rd.HardState, rd.Entries, rd.Snapshot, rd.Messages, rd.CommittedEntries, rb.confState.String())
	rb.store.saveEntriesAndState(rd.Entries, rd.HardState)

	if !raft.IsEmptySnap(rd.Snapshot) {
		if err := rb.store.saveSnap(rd.Snapshot); err != nil {
			rb.logger.Errorf("Couldn't save snapshot: %s", err.Error())
			return false
		}

		rb.publishSnapshot(rd.Snapshot)
	}

	sendingErrors := rb.transport.sendMessages(rd.Messages)
	if sendingErrors != nil {
		for _, failedMsg := range sendingErrors.failedMessages {
			// TODO - think this through
			// rb.logger.Errorf("Reporting raft [%d %x] unreachable", failedMsg.To, failedMsg.To)
			// rb.raftNode.ReportUnreachable(failedMsg.To)
			if isMsgSnap(failedMsg) {
				rb.logger.Errorf("Reporting snapshot failure for raft [%d %x]", failedMsg.To, failedMsg.To)
				rb.raftNode.ReportSnapshot(failedMsg.To, raft.SnapshotFailure)
			}
		}

		for _, snapMsg := range sendingErrors.succeededSnapshotMessages {
			rb.raftNode.ReportSnapshot(snapMsg.To, raft.SnapshotFinish)
		}
	}

	if ok := rb.publishEntries(rb.entriesToApply(rd.CommittedEntries)); !ok {
		return false
	}
	rb.maybeTriggerSnapshot()
	rb.raftNode.Advance()
	return true
}

func (rb *raftBackend) publishSnapshot(snap raftpb.Snapshot) {
	if snap.Metadata.Index <= rb.appliedIndex {
		rb.logger.Errorf("Snapshot too old! Snap index [%d] applied index [%d]", snap.Metadata.Index, rb.appliedIndex)
		return
	}

	// TODO - think this through
	// BEWARE: There might be a race condition in here.
	// In theory it's possible for the consumer to take some time when
	// processing this nil (aka reload a snapshot).
	// If there are back to back messages with snapshots, the second snapshot could
	// override the first snapshot in our store.
	// Therefore the first attempt to load the snapshot will actually load the
	// second snapshot and everything might go down the drain at that point.
	// However, the snapshot method of this object implements the callback
	// the consumer will eventually call.
	// It's possible to synchronize this call to stop the raft state machine
	// while the consumer is loading the snapshot.
	if rb.isInteractive {
		rb.commitChan <- nil
	}

	rb.confState = snap.Metadata.ConfState
	rb.snapshotIndex = snap.Metadata.Index
	rb.appliedIndex = snap.Metadata.Index
}

func (rb *raftBackend) maybeTriggerSnapshot() {
	if rb.appliedIndex-rb.snapshotIndex < rb.snapshotFrequency {
		// we didn't collect enough entries yet to warrant
		// a new snapshot
		return
	}

	// get snapshot from data structure
	data, err := rb.snapshotProvider()
	if err != nil {
		rb.logger.Errorf("Couldn't get a snapshot: %s", err.Error())
		return
	}

	// bake snapshot object by tossing all the metadata and byte arrays in there
	snap, err := rb.bakeNewSnapshot(data)
	if err != nil {
		rb.logger.Errorf("Can't bake new snapshot: %s", err.Error())
		return
	}

	// save the snapshot
	err = rb.store.saveSnap(snap)
	if err != nil {
		rb.logger.Errorf("Can't save new snapshot: %s", err.Error())
		return
	}

	// drop all log entries before the snapshot index
	err = rb.store.dropLogEntriesBeforeIndex(snap.Metadata.Index)
	if err != nil {
		rb.logger.Errorf("Couldn't delete old log entries: %s", err.Error())
	}

	// drop old snapshots if applicable
	err = rb.store.dropOldSnapshots(rb.numberOfSnapshotsToKeep)
	if err != nil {
		rb.logger.Errorf("Couldn't delete old snaphots: %s", err.Error())
	}
}

func (rb *raftBackend) bakeNewSnapshot(data []byte) (raftpb.Snapshot, error) {
	hardState, confState, err := rb.store.InitialState()
	if err != nil {
		return raftpb.Snapshot{}, err
	}

	metadata := raftpb.SnapshotMetadata{
		ConfState: confState,
		Index:     rb.appliedIndex,
		Term:      hardState.Term,
	}

	return raftpb.Snapshot{
		Data:     data,
		Metadata: metadata,
	}, nil
}

func (rb *raftBackend) entriesToApply(ents []raftpb.Entry) []raftpb.Entry {
	if len(ents) == 0 {
		return make([]raftpb.Entry, 0)
	}

	firstIdx := ents[0].Index
	if firstIdx > rb.appliedIndex+1 {
		// if I'm getting invalid data, I'm shutting down
		rb.logger.Errorf("first index of committed entry [%d] should <= progress.appliedIndex[%d] !", firstIdx, rb.appliedIndex)
		rb.stop()
	}

	// if I get things that I didn't ask for, I sort them out
	if rb.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		return ents[rb.appliedIndex-firstIdx+1:]
	}

	return ents
}

func (rb *raftBackend) publishEntries(ents []raftpb.Entry) bool {
	rb.logger.Debugf("Num entries to publish: %d", len(ents))
	for idx := range ents {
		switch ents[idx].Type {
		case raftpb.EntryNormal:
			// if this node is detached (aka not interactive)
			// don't send anything to the commit channel
			if rb.isInteractive && len(ents[idx].Data) > 0 {
				// ignore empty messages
				select {
				case rb.commitChan <- ents[idx].Data:
					rb.logger.Debugf("Publishing entry with size %d to %v", len(ents[idx].Data), rb.commitChan)
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
			rb.latestConfChangeId = cc.ID
			switch cc.Type {
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == rb.raftId {
					rb.logger.Error("I've been removed from the cluster! Shutting down.")
					// this false will cause the code to call stop eventually
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

// called by the snapshot provider handed to the consumer
func (rb *raftBackend) snapshot() ([]byte, error) {
	snap, err := rb.store.Snapshot()
	return snap.Data, err
}

// I will need to have a long inner monologue about
// when to add a voter and when to add a learner.
// Adding voters comes with the increasing overhead
// of the raft protocol.
// Adding learners and never promoting them to
// voters puts us in danger to lose data.
func (rb *raftBackend) addRaftToMyGroup(ctx context.Context, newRaftId uint64) error {
	cc := raftpb.ConfChange{
		Type:   raftpb.ConfChangeAddLearnerNode,
		ID:     rb.latestConfChangeId + 1,
		NodeID: newRaftId,
	}
	return rb.raftNode.ProposeConfChange(ctx, cc)
}

// can be called any number of times
// but will only do work the first time
func (rb *raftBackend) _stop(shouldCloseStopChan bool) {
	if atomic.CompareAndSwapInt32(&rb.alreadyClosed, int32(0), int32(1)) {
		// stop the raft node
		rb.raftNode.Stop()
		// stop the raft state machine
		if shouldCloseStopChan {
			close(rb.stopChan)
		}

		if rb.commitChan != nil {
			close(rb.commitChan)
		}

		if rb.errorChan != nil {
			close(rb.errorChan)
		}
	}
}

func (rb *raftBackend) stop() {
	rb.logger.Warnf("Shutting down raft backend %d %x", rb.raftId, rb.raftId)
	// TODO - this is an interesting idea
	// removing myself out of a raft group will make this raft node shut down
	// evatually after the conf change has been committed
	// after the raft state machine processed this change, _stop() is being called
	// this way the node is removed the most graceful way possible out of the group
	err := rb.raftNode.ProposeConfChange(context.TODO(), raftpb.ConfChange{
		Type:   raftpb.ConfChangeRemoveNode,
		NodeID: rb.raftId,
		ID:     rb.latestConfChangeId,
	})
	if err != nil {
		rb.logger.Errorf("Can't remove myself out of raft group: %s", err.Error())
	}
	// call internal stop
	rb._stop(true)
}
