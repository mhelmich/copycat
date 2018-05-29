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
	"sync"

	"github.com/mhelmich/copycat/pb"
	"google.golang.org/grpc"
)

type raftBackendCache struct {
	membership          *membership
	raftIdToRaftBackend *sync.Map
	addressToConnection *sync.Map
}

func newRaftBackendCache(m *membership) *raftBackendCache {
	return &raftBackendCache{
		membership:          m,
		raftIdToRaftBackend: &sync.Map{},
		addressToConnection: &sync.Map{},
	}
}

func (rbc *raftBackendCache) newDetachedRaftBackend(raftId uint64, config *Config) {}

func (rbc *raftBackendCache) newInteractiveRaftBackend(config *Config, peers []pb.Peer, provider SnapshotProvider) (*raftBackend, error) {
	return nil, nil
}

func (rbc *raftBackendCache) getConnectionForRaftId(raftId uint64) *grpc.ClientConn {
	return nil
}
