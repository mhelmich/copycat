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
	"encoding/binary"
	"strconv"

	log "github.com/sirupsen/logrus"

	"github.com/coreos/etcd/raft/raftpb"
	"github.com/mhelmich/copycat/pb"
)

type msgAndStream struct {
	stream pb.RaftTransportService_SendServer
	msg    *raftpb.Message
}

// converts bytes to an unsinged 64 bit integer
func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

// converts a uint64 to a byte slice
func uint64ToBytes(u uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, u)
	return buf
}

func uint64ToString(i uint64) string {
	return strconv.FormatUint(i, 10)
}

func stringToUint64(s string) uint64 {
	i, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Panicf("Can't prase string %s to uint64: %s", s, err.Error())
	}
	return i
}

func isMsgSnap(m raftpb.Message) bool {
	return m.Type == raftpb.MsgSnap
}

func randomRaftId() uint64 {
	bites := make([]byte, 8)
	_, err := rand.Read(bites)
	if err != nil {
		log.Panicf("Can't read from random: %s", err.Error())
	}
	return bytesToUint64(bites)
}
