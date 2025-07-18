// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package beat

import (
	"time"

	"github.com/gofrs/uuid/v5"
	"go.opentelemetry.io/collector/consumer"

	"github.com/elastic/elastic-agent-libs/logp"
)

// Info stores a beats instance meta data.
type Info struct {
	Beat             string    // The actual beat's name
	IndexPrefix      string    // The beat's index prefix in Elasticsearch.
	Version          string    // The beat version. Defaults to the libbeat version when an implementation does not set a version
	ElasticLicensed  bool      // Whether the beat is licensed under and Elastic License
	Name             string    // configured beat name
	Hostname         string    // hostname
	FQDN             string    // FQDN
	ID               uuid.UUID // ID assigned to beat machine
	EphemeralID      uuid.UUID // ID assigned to beat process invocation (PID)
	FirstStart       time.Time // The time of the first start of the Beat.
	StartTime        time.Time // The time of last start of the Beat. Updated when the Beat is started or restarted.
	UserAgent        string    // A string of the user-agent that can be passed to any outputs or network connections
	FIPSDistribution bool      // If the beat was compiled as a FIPS distribution.

	LogConsumer          consumer.Logs // otel log consumer
	UseDefaultProcessors bool          // Whether to use the default processors
	Logger               *logp.Logger
}

func (i Info) FQDNAwareHostname(useFQDN bool) string {
	if useFQDN {
		return i.FQDN
	}

	return i.Hostname
}
