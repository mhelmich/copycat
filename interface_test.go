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
	"encoding/json"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/mhelmich/copycat/pb"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func __TestCopyCatBasic(t *testing.T) {
	cfg := DefaultConfig()
	cc, err := NewCopyCat(cfg)
	assert.Nil(t, err)

	tc := loadTestCat(cc)
	assert.Nil(t, tc)

	tc.put("narf", "narf")

	cc.Shutdown()
}

func loadTestCat(cc CopyCat) *testCat {
	cat := &testCat{}
	catId := uint64(99)
	cat.proposeCh, cat.commitCh, cat.errorCh, cat.snapshotConsumer = cc.ConnectToDataStructure(catId, cat.providerSnapshot)
	go cat.serveChannel()
	return cat
}

type testCat struct {
	data             map[string]string
	proposeCh        chan<- []byte
	commitCh         <-chan []byte
	errorCh          <-chan error
	snapshotConsumer SnapshotConsumer
}

type kv struct {
	key   string
	value string
}

func (c *testCat) serveChannel() {
	for {
		select {
		case data, ok := <-c.commitCh:
			if !ok {
				return
			}

			// the committed entry is nil, that means we need to reload the
			// data structure from a consistent snapshot
			if data == nil {
				// TODO reload map from snapshot
				bites, err := c.snapshotConsumer()
<<<<<<< HEAD
				if err != nil {
					log.Errorf("Error getting snapshot: %s", err.Error())
				}

				tcData := &pb.TestCat{}
				err = proto.Unmarshal(bites, tcData)
				if err != nil {
					log.Errorf("Error unmarshaling snapshot: %s", err.Error())
				}

				c.data = tcData.M
=======
				tcData := &pb.TestCat{}
				proto.Unmarshal(bites, tcData)
				c.data = tcData.M
				log.Infof("%d %s", len(bites), err)
>>>>>>> 11afee7bc178e6b0e2f004344ac580ad4468ad46
			}

			newOp := &kv{}
			err := json.Unmarshal(data, newOp)
			if err != nil {
				log.Errorf("Can't unmarshal operation: %s", err.Error())
			}

			if newOp.value == "" {
				delete(c.data, newOp.key)
			} else {
				c.data[newOp.key] = newOp.value
			}

		case err, ok := <-c.errorCh:
			if !ok {
				return
			}
			log.Errorf("Ran into error: %s", err.Error())
		}
	}
}

func (c *testCat) get(key string) string {
	return c.data[key]
}

func (c *testCat) put(key, value string) {
	item := &kv{
		key:   key,
		value: value,
	}

	bites, err := json.Marshal(item)
	if err != nil {
		log.Errorf("Can't marshal op: %s", err.Error())
	}

	c.proposeCh <- bites
}

func (c *testCat) providerSnapshot() ([]byte, error) {
	return json.Marshal(&pb.TestCat{M: c.data})
}
