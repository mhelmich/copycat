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

	raftpb "github.com/coreos/etcd/raft/raftpb"
	mock "github.com/stretchr/testify/mock"
)

// Thanks mockery!

type mockRaftBackend struct {
	mock.Mock
}

func (rb *mockRaftBackend) step(ctx context.Context, msg raftpb.Message) error {
	args := rb.Called(ctx, msg)
	return args.Error(0)
}

func (rb *mockRaftBackend) stop() {
	rb.Called()
}
