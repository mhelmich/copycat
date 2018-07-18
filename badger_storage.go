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
	"bytes"
	"os"

	"github.com/coreos/etcd/raft"
	raftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/dgraph-io/badger"
	log "github.com/sirupsen/logrus"
)

const (
	entriesDatabaseName = "entries.db/"
	staticsDatabaseName = "statics.db/"
)

type badgerStorage struct {
	entriesDB *badger.DB
	staticsDB *badger.DB
	logger    *log.Entry
}

func badgerStorageExists(dir string) bool {
	return bsExists(dir+entriesDatabaseName) && bsExists(dir+staticsDatabaseName)
}

func bsExists(dir string) bool {
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return false
	} else if err != nil {
		return false
	}

	return true
}

func openBadgerStorage(dir string, parentLogger *log.Entry) (*badgerStorage, error) {
	if !badgerStorageExists(dir) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	opts1 := badger.DefaultOptions
	opts1.Dir = dir + entriesDatabaseName
	opts1.ValueDir = dir + entriesDatabaseName
	entriesDB, err := badger.Open(opts1)
	if err != nil {
		return nil, err
	}

	opts2 := badger.DefaultOptions
	opts2.Dir = dir + staticsDatabaseName
	opts2.ValueDir = dir + staticsDatabaseName
	staticsDB, err := badger.Open(opts2)
	if err != nil {
		return nil, err
	}

	if parentLogger == nil {
		parentLogger = log.WithFields(log.Fields{
			"component": "storage",
		})
	} else {
		parentLogger = parentLogger.WithFields(log.Fields{
			"component": "storage",
		})
	}

	return &badgerStorage{
		entriesDB: entriesDB,
		staticsDB: staticsDB,
		logger:    parentLogger,
	}, nil
}

func (bs *badgerStorage) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	// create pointers
	// both pointers will be set within the transaction that's guaranteed
	var hs *raftpb.HardState
	var cs *raftpb.ConfState
	err := bs.staticsDB.View(func(txn *badger.Txn) error {
		var txnErr1 error
		var txnErr2 error
		// breaking up regular work and check pattern to make sure
		// that pointers for hs and cs are set either way
		hs, txnErr1 = bs.loadHardState(txn)
		cs, txnErr2 = bs.loadConfState(txn)

		if txnErr1 != nil {
			return txnErr1
		}

		if txnErr2 != nil {
			return txnErr2
		}

		return nil
	})

	return *hs, *cs, err
}

// NB: can't return nil and must handle "key not found" errors
func (bs *badgerStorage) loadHardState(txn *badger.Txn) (*raftpb.HardState, error) {
	hs := &raftpb.HardState{}
	var item *badger.Item
	var txnErr error
	var bites []byte
	item, txnErr = txn.Get(hardStateKey)
	if txnErr == badger.ErrKeyNotFound {
		return hs, nil
	} else if txnErr != nil {
		return hs, txnErr
	}

	bites, txnErr = item.Value()
	if txnErr != nil {
		return hs, txnErr
	}

	txnErr = hs.Unmarshal(bites)
	if txnErr != nil {
		return hs, txnErr
	}

	return hs, nil
}

// NB: can't return nil and must handle "key not found" errors
func (bs *badgerStorage) loadConfState(txn *badger.Txn) (*raftpb.ConfState, error) {
	cs := &raftpb.ConfState{}
	var item *badger.Item
	var txnErr error
	var bites []byte
	item, txnErr = txn.Get(confStateKey)
	if txnErr == badger.ErrKeyNotFound {
		return cs, nil
	} else if txnErr != nil {
		return cs, txnErr
	}

	bites, txnErr = item.Value()
	if txnErr != nil {
		return cs, txnErr
	}

	txnErr = cs.Unmarshal(bites)
	if txnErr != nil {
		return cs, txnErr
	}

	return cs, nil
}

func (bs *badgerStorage) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	min := uint64ToBytes(lo)
	max := uint64ToBytes(hi)
	entries := make([]raftpb.Entry, int(hi-lo))
	idx := 0

	err := bs.entriesDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = int(hi - lo)
		it := txn.NewIterator(opts)
		defer it.Close()

		var runningByteSize uint64
		var bites []byte
		var txnErr error
		for it.Seek(min); it.Valid() && idx < int(hi-lo); it.Next() {
			if bytes.Compare(it.Item().Key(), max) > 0 {
				// if we reached max, we're done
				return nil
			}

			bites, txnErr = it.Item().Value()
			if txnErr != nil {
				return txnErr
			}

			bitesSize := uint64(len(bites))
			if runningByteSize+bitesSize > maxSize {
				return nil
			}

			runningByteSize += bitesSize
			e := &raftpb.Entry{}
			txnErr = e.Unmarshal(bites)
			if txnErr != nil {
				return txnErr
			}

			entries[idx] = *e
			idx++
		}

		return nil
	})

	return entries[:idx], err
}

func (bs *badgerStorage) Term(i uint64) (uint64, error) {
	var term uint64
	err := bs.entriesDB.View(func(txn *badger.Txn) error {
		var item *badger.Item
		var txnErr error
		var bites []byte
		item, txnErr = txn.Get(uint64ToBytes(i))
		if txnErr != nil {
			return txnErr
		}

		bites, txnErr = item.Value()
		if txnErr != nil {
			return txnErr
		}

		e := &raftpb.Entry{}
		txnErr = e.Unmarshal(bites)
		if txnErr != nil {
			return txnErr
		}

		term = e.Term
		return nil
	})
	if err == badger.ErrKeyNotFound {
		return 0, nil
	}

	return term, err
}

func (bs *badgerStorage) LastIndex() (uint64, error) {
	var index uint64
	err := bs.entriesDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Reverse = true
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		if !it.Valid() {
			return badger.ErrKeyNotFound
		}

		index = bytesToUint64(it.Item().Key())
		return nil
	})

	if err == badger.ErrKeyNotFound {
		return 0, nil
	}

	return index, err
}

func (bs *badgerStorage) FirstIndex() (uint64, error) {
	var index uint64
	err := bs.entriesDB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		it.Rewind()
		if !it.Valid() {
			return badger.ErrKeyNotFound
		}

		index = bytesToUint64(it.Item().Key())
		return nil
	})

	if err == badger.ErrKeyNotFound {
		return 1, nil
	}

	return index, err
}

func (bs *badgerStorage) Snapshot() (raftpb.Snapshot, error) {
	snap := &raftpb.Snapshot{}
	err := bs.staticsDB.View(func(txn *badger.Txn) error {
		var item *badger.Item
		var txnErr error
		var bites []byte
		// TODO - fix this maybe
		item, txnErr = txn.Get(snapshotsBucket)
		if txnErr != nil {
			return txnErr
		}

		bites, txnErr = item.Value()
		if txnErr != nil {
			return txnErr
		}

		txnErr = snap.Unmarshal(bites)
		if txnErr != nil {
			return txnErr
		}
		return nil
	})

	if err == badger.ErrKeyNotFound {
		return *snap, nil
	}

	return *snap, err
}

func (bs *badgerStorage) saveConfigState(confState raftpb.ConfState) error {
	bites, err := confState.Marshal()
	if err != nil {
		return err
	}

	err = bs.staticsDB.Update(func(txn *badger.Txn) error {
		return txn.Set(confStateKey, bites)
	})
	return err
}

func (bs *badgerStorage) saveEntriesAndState(entries []raftpb.Entry, hardState raftpb.HardState) error {
	var err error
	var bites []byte

	bites, err = hardState.Marshal()
	if err != nil {
		return err
	}

	if !raft.IsEmptyHardState(hardState) {
		err = bs.staticsDB.Update(func(txn *badger.Txn) error {
			return txn.Set(hardStateKey, bites)
		})
		if err != nil {
			return err
		}
	}

	err = bs.entriesDB.Update(func(txn *badger.Txn) error {
		var bites []byte
		var txnErr error
		for idx := range entries {
			bites, txnErr = entries[idx].Marshal()
			if txnErr != nil {
				return txnErr
			}

			txnErr = txn.Set(uint64ToBytes(entries[idx].Index), bites)
			if txnErr != nil {
				return txnErr
			}
		}
		return nil
	})
	return err
}

func (bs *badgerStorage) dropLogEntriesBeforeIndex(index uint64) error {
	err := bs.entriesDB.Update(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Reverse = true
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(uint64ToBytes(index)); it.Valid(); it.Next() {
			txnErr := txn.Delete(it.Item().Key())
			if txnErr != nil {
				return txnErr
			}
		}

		return nil
	})

	// even though it says no multiple GCs are allowed
	// it seems badger failes gracefully by returning an error
	// therefore there's no harm in calling it whenever we feel like
	// as long as we don't depend in the return value
	defer bs.staticsDB.RunValueLogGC(0.5)
	defer bs.entriesDB.RunValueLogGC(0.5)

	return err
}

func (bs *badgerStorage) saveSnap(snap raftpb.Snapshot) error {
	bites, err := snap.Marshal()
	if err != nil {
		return err
	}

	err = bs.staticsDB.Update(func(txn *badger.Txn) error {
		return txn.Set(snapshotsBucket, bites)
	})
	return err
}

func (bs *badgerStorage) dropOldSnapshots(numberOfSnapshotsToKeep int) error {
	return nil
}

func (bs *badgerStorage) close() {
	bs.entriesDB.Close()
	bs.staticsDB.Close()
}
