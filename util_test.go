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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConversions(t *testing.T) {
	i := randomRaftId()

	bites := uint64ToBytes(i)
	assert.Equal(t, i, bytesToUint64(bites))

	str := uint64ToString(i)
	assert.Equal(t, i, stringToUint64(str))
}
