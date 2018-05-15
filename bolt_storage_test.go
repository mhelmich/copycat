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
	"crypto/rand"
	"os"
	"testing"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/oklog/ulid"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestBoltStorageBasic(t *testing.T) {
	dir := "./" + ulid.MustNew(ulid.Now(), rand.Reader).String() + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)

	hardState, confState, err := store.InitialState()
	assert.Nil(t, err)
	assert.NotNil(t, hardState)
	assert.NotNil(t, confState)

	store.close()
	os.RemoveAll(dir)
}

func TestBoltStorageSaveSnapshot(t *testing.T) {
	dir := "./" + ulid.MustNew(ulid.Now(), rand.Reader).String() + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)

	snap, err := store.Snapshot()
	assert.Nil(t, err)
	assert.NotNil(t, snap)

	snapToWrite := &raftpb.Snapshot{}
	snapToWrite.Metadata = raftpb.SnapshotMetadata{
		Index: uint64(99),
		Term:  uint64(77),
		ConfState: raftpb.ConfState{
			Nodes:    make([]uint64, 3),
			Learners: make([]uint64, 5),
		},
	}
	snapToWrite.Data = make([]byte, 33)
	err = store.saveSnap(*snapToWrite)
	assert.Nil(t, err)

	snap2, err := store.Snapshot()
	assert.Nil(t, err)
	assert.NotNil(t, snap2)
	assert.Equal(t, snapToWrite.Metadata.Index, snap2.Metadata.Index)
	assert.Equal(t, snapToWrite.Metadata.Term, snap2.Metadata.Term)

	store.close()
	os.RemoveAll(dir)
}

func TestBoltStorageFunWithEntries(t *testing.T) {
	dir := "./" + ulid.MustNew(ulid.Now(), rand.Reader).String() + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)

	count := 97
	entsToStore := make([]raftpb.Entry, count)
	for i := 0; i < count; i++ {
		bites := make([]byte, i)
		rand.Read(bites)
		entsToStore[i] = raftpb.Entry{
			Term:  uint64(i),
			Index: uint64(i),
			Type:  raftpb.EntryNormal,
			Data:  bites,
		}
	}

	st := raftpb.HardState{
		Term:   uint64(101),
		Vote:   uint64(4),
		Commit: uint64(97),
	}

	err = store.saveEntriesAndState(entsToStore, st)
	assert.Nil(t, err)

	first, err := store.FirstIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), first)

	last, err := store.LastIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(count-1), last)

	term, err := store.Term(uint64(37))
	assert.Nil(t, err)
	assert.Equal(t, uint64(37), term)

	ents, err := store.Entries(uint64(37), uint64(37), uint64(37))
	assert.Nil(t, err)
	assert.Equal(t, 0, len(ents))

	ents, err = store.Entries(uint64(37), uint64(38), uint64(37))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(ents))
	assert.Equal(t, uint64(37), ents[0].Index)

	ents, err = store.Entries(uint64(37), uint64(40), uint64(37))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(ents))
	assert.Equal(t, uint64(37), ents[0].Index)

	ents, err = store.Entries(uint64(37), uint64(40), uint64(57))
	assert.Nil(t, err)
	// still only one because the byte size limitation kicks in
	assert.Equal(t, 1, len(ents))
	assert.Equal(t, uint64(37), ents[0].Index)

	ents, err = store.Entries(uint64(37), uint64(40), uint64(97))
	assert.Nil(t, err)
	assert.Equal(t, 2, len(ents))
	assert.Equal(t, uint64(37), ents[0].Index)
	assert.Equal(t, uint64(38), ents[1].Index)

	// byte size is an exact match
	ents, err = store.Entries(uint64(37), uint64(40), uint64(138))
	assert.Nil(t, err)
	assert.Equal(t, 3, len(ents))
	assert.Equal(t, uint64(37), ents[0].Index)
	assert.Equal(t, uint64(38), ents[1].Index)
	assert.Equal(t, uint64(39), ents[2].Index)

	store.close()
	os.RemoveAll(dir)
}

func TestBoltStorageNoKeysFound(t *testing.T) {
	dir := "./" + ulid.MustNew(ulid.Now(), rand.Reader).String() + "/"
	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)

	first, err := store.FirstIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), first)
	last, err := store.LastIndex()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), last)

	store.close()
	os.RemoveAll(dir)
}

func TestBoltStorageExistingDB(t *testing.T) {
	dir := "./" + ulid.MustNew(ulid.Now(), rand.Reader).String() + "/"

	b := storageExists(dir)
	assert.False(t, b)

	store, err := openBoltStorage(dir, log.WithFields(log.Fields{}))
	assert.Nil(t, err)

	b = storageExists(dir)
	assert.True(t, b)

	store.close()
	os.RemoveAll(dir)
}
