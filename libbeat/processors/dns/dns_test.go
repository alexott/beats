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

package dns

import (
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/elastic-agent-libs/logp/logptest"
	"github.com/elastic/elastic-agent-libs/mapstr"
	"github.com/elastic/elastic-agent-libs/monitoring"
)

func TestDNSProcessorRun(t *testing.T) {
	c := defaultConfig()
	c.Type = typePTR
	p := &processor{
		config:   c,
		resolver: &stubResolver{},
		log:      logptest.NewTestingLogger(t, logName),
	}
	p.reverseFlat = map[string]string{
		"source.ip": "source.domain",
	}
	t.Log(p.String())

	t.Run("default", func(t *testing.T) {
		event, err := p.Run(&beat.Event{
			Fields: mapstr.M{
				"source.ip": gatewayIP,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		v, _ := event.GetValue("source.domain")
		assert.Equal(t, gatewayName, v)
	})

	const forwardDomain = "www." + gatewayName
	t.Run("append", func(t *testing.T) {
		p.Action = actionAppend

		event, err := p.Run(&beat.Event{
			Fields: mapstr.M{
				"source.ip":     gatewayIP,
				"source.domain": forwardDomain,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		v, _ := event.GetValue("source.domain")
		assert.ElementsMatch(t,
			[]string{gatewayName, forwardDomain},
			v)
	})

	t.Run("replace", func(t *testing.T) {
		p.Action = actionReplace

		event, err := p.Run(&beat.Event{
			Fields: mapstr.M{
				"source.ip":     gatewayIP,
				"source.domain": forwardDomain,
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		v, _ := event.GetValue("source.domain")
		assert.Equal(t, gatewayName, v)
	})

	t.Run("metadata target", func(t *testing.T) {
		config := defaultConfig()
		config.Type = typePTR
		config.reverseFlat = map[string]string{
			"@metadata.ip": "@metadata.domain",
		}

		p := &processor{
			config:   config,
			resolver: &stubResolver{},
			log:      logptest.NewTestingLogger(t, logName),
		}

		event := &beat.Event{
			Meta: mapstr.M{
				"ip": gatewayIP,
			},
		}

		expMeta := mapstr.M{
			"ip":     gatewayIP,
			"domain": gatewayName,
		}

		newEvent, err := p.Run(event)
		assert.NoError(t, err)
		assert.Equal(t, expMeta, newEvent.Meta)
		assert.Equal(t, event.Fields, newEvent.Fields)
	})
}

func TestDNSProcessorTagOnFailure(t *testing.T) {
	p := &processor{
		config:   defaultConfig(),
		resolver: &stubResolver{},
		log:      logptest.NewTestingLogger(t, logName),
	}
	p.TagOnFailure = []string{"_lookup_failed"}
	p.reverseFlat = map[string]string{
		"source.ip":      "source.domain",
		"destination.ip": "destination.domain",
	}
	t.Log(p.String())

	event, err := p.Run(&beat.Event{
		Fields: mapstr.M{
			"source.ip":      "192.0.2.1",
			"destination.ip": "192.0.2.2",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	v, _ := event.GetValue("tags")
	if assert.Len(t, v, 1) {
		assert.ElementsMatch(t, v, p.TagOnFailure)
	}
}

func TestDNSProcessorRunInParallel(t *testing.T) {
	// This is a simple smoke test to make sure that there are no concurrency
	// issues. It is most effective when run with the race detector.

	conf := defaultConfig()
	reg := monitoring.NewRegistry()
	cache, err := newLookupCache(reg, conf.cacheConfig, &stubResolver{})
	if err != nil {
		t.Fatal(err)
	}
	p := &processor{config: conf, resolver: cache, log: logptest.NewTestingLogger(t, logName)}
	p.reverseFlat = map[string]string{"source.ip": "source.domain"}

	const numGoroutines = 10
	const numEvents = 500

	// Start several goroutines.
	g, _ := errgroup.WithContext(context.Background())

	for i := 0; i < numGoroutines; i++ {
		g.Go(func() error {
			// Execute processor.
			for i := 0; i < numEvents; i++ {
				_, err := p.Run(&beat.Event{
					Fields: mapstr.M{
						"source.ip": "192.168.0." + strconv.Itoa(i%256),
					},
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
	}

	err = g.Wait()
	if err != nil {
		t.Fatal(err)
	}
}
