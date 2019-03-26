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
	"os"

	log "github.com/sirupsen/logrus"
)

const (
	defaultCopyCatPort    = 11983
	defaultCopyCatDataDir = "./copycat.db/"
)

func DefaultConfig() *Config {
	host, _ := os.Hostname()
	return &Config{
		Hostname:       host,
		CopyCatPort:    defaultCopyCatPort,
		GossipPort:     defaultCopyCatPort + 1000,
		CopyCatDataDir: defaultCopyCatDataDir,
	}
}

// Config is the public configuration struct that needs to be passed into the constructor function.
type Config struct {
	// Advertized host name
	Hostname string
	// Port of the CopyCat server. The server runs the management interface and the raft state machine.
	CopyCatPort int
	// Port on which CopyCat gossips about cluster metadata.
	GossipPort int
	// Directory under which this CopyCat instance will put all data it's collecting.
	CopyCatDataDir string
	// Simple address of at least one CopyCat peer to contact. The format is "<machine>:<copycat_port>".
	// If you leave this empty or nil, this node will start a brandnew cluster.
	PeersToContact []string
	// Arbitrary string identifying a geo location. CopyCat will try to distribute data across multiple
	// geo locations to protect data against dependent failures such as power outages, etc.
	// Even though the API doesn't prescribe a string size, shorter is better.
	// A data center is such an arbitrary geo location.
	DataCenter string
	// A rack is a section within a data center. Even though you might not know which rack your nodes live in,
	// this can be used to create a second hierarchy for data placement.
	Rack string

	// Populated internally.
	raftTransport raftTransport
	logger        *log.Entry
}
