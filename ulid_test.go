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
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func TestUlidBasic(t *testing.T) {
	ulid1, err := newUlid()
	assert.Nil(t, err)
	bites1, err := ulid1.toBytes()
	assert.Nil(t, err)
	ulid2, err := parseUlid(bites1)
	assert.Nil(t, err)
	assert.Equal(t, ulid1.String(), ulid2.String())
	log.Infof("%s %s", ulid1.String(), ulid2.String())

	// make the time break over
	time.Sleep(time.Millisecond)

	ulid3, err := newUlid()
	assert.Nil(t, err)
	assert.True(t, ulid1.compareTo(ulid3) < 0)
	log.Infof("%s %s", ulid1.String(), ulid3.String())
}

/////////////////////////////////////////////////////////
/////////////////////////////////////////////////
//////////////////////////////////////////
// TODO -> figure out what to do here
// it seems the latency is dominated by reading from
// random anyways ... as it should
// therefore there's no real point in micro-optimizing too much
func _TestUlidBitShifting(t *testing.T) {
	buf := make([]uint8, 2)
	rand.Reader.Read(buf)
	now := nowUnixUtc()
	nowB := uint64ToBytes(now)

	log.Infof("%v", buf)
	log.Infof("%v %d", nowB, now)
	nowB[6] = buf[0]
	nowB[7] = buf[1]
	log.Infof("I want this: %v %d", nowB, bytesToUint64(nowB))

	b6 := uint8(now >> 8)
	b7 := uint8(now)
	log.Infof("Last two bytes: %d %d", b6, b7)

	and := uint64(0) | uint64(0)<<8 | uint64(255)<<16 | uint64(255)<<24 |
		uint64(255)<<32 | uint64(255)<<40 | uint64(255)<<48 | uint64(255)<<56

	cleanEnd := now & and
	log.Infof("clean end: %v %d", uint64ToBytes(cleanEnd), cleanEnd)

	or := uint64(buf[1]) | uint64(buf[0])<<8 | uint64(0)<<16 | uint64(0)<<24 |
		uint64(0)<<32 | uint64(0)<<40 | uint64(0)<<48 | uint64(0)<<56

	whatIWant := cleanEnd | or
	log.Infof("what I want: %v %d", uint64ToBytes(whatIWant), whatIWant)
}

func _BenchmarkByteArrayShifting(b *testing.B) {
	for n := 0; n < b.N; n++ {
		buf := make([]uint8, 2)
		rand.Reader.Read(buf)
		now := nowUnixUtc()
		nowB := uint64ToBytes(now)
		nowB[6] = buf[0]
		nowB[7] = buf[1]
	}
}

func _BenchmarkBitShifting(b *testing.B) {
	for n := 0; n < b.N; n++ {
		buf := make([]uint8, 2)
		rand.Reader.Read(buf)
		now := nowUnixUtc()
		// and := uint64(0) | uint64(0)<<8 | uint64(255)<<16 | uint64(255)<<24 |
		// 	uint64(255)<<32 | uint64(255)<<40 | uint64(255)<<48 | uint64(255)<<56
		// cleanEnd := now & and
		cleanEnd := now & uint64(18446744073709486080)
		or := uint64(buf[1]) | uint64(buf[0])<<8 | uint64(0)<<16 | uint64(0)<<24 |
			uint64(0)<<32 | uint64(0)<<40 | uint64(0)<<48 | uint64(0)<<56
		narf := cleanEnd | or
		_ = narf
	}
}
