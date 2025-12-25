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

package zerobus

import (
	"context"
	"fmt"
	"os"
	"sync"

	zerobus "github.com/databricks/zerobus-sdk-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/codec"
	"github.com/elastic/beats/v7/libbeat/outputs/codec/json"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
)

func init() {
	outputs.RegisterType("zerobus", makeZerobus)
}

// zerobusOutput implements the outputs.Client interface
type zerobusOutput struct {
	log      *logp.Logger
	config   *Config
	codec    codec.Codec
	observer outputs.Observer
	index    string

	// SDK components
	sdk    *zerobus.ZerobusSdk
	stream *zerobus.ZerobusStream

	// Proto support
	messageDescriptor protoreflect.MessageDescriptor

	// Concurrency control
	workerSem chan struct{}
}

// makeZerobus creates a new Zerobus output
func makeZerobus(
	_ outputs.IndexManager,
	beat beat.Info,
	observer outputs.Observer,
	cfg *config.C,
) (outputs.Group, error) {
	config, err := readConfig(cfg)
	if err != nil {
		return outputs.Fail(err)
	}

	if err := config.Validate(); err != nil {
		return outputs.Fail(fmt.Errorf("zerobus output configuration validation failed: %w", err))
	}

	// Create codec
	var enc codec.Codec
	if config.Codec.Namespace.IsSet() {
		enc, err = codec.CreateEncoder(beat, config.Codec)
		if err != nil {
			return outputs.Fail(fmt.Errorf("failed to create codec: %w", err))
		}
	} else {
		// Use default JSON codec
		enc = json.New(beat.Version, json.Config{
			Pretty:     false,
			EscapeHTML: false,
		})
	}

	// Create output instance
	output, err := newZerobusOutput(beat, observer, config, enc)
	if err != nil {
		return outputs.Fail(fmt.Errorf("zerobus output initialization failed: %w", err))
	}

	return outputs.Success(config.Queue, config.BatchSize, config.Retry.Max, nil, beat.Logger, output)
}

// newZerobusOutput creates a new zerobusOutput instance
func newZerobusOutput(
	beat beat.Info,
	observer outputs.Observer,
	config *Config,
	codec codec.Codec,
) (*zerobusOutput, error) {
	log := beat.Logger.Named("zerobus")

	// Create SDK instance
	sdk, err := zerobus.NewZerobusSdk(
		config.buildURL(),
		config.WorkspaceURL,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create SDK: %w", err)
	}

	// Load proto descriptor if in proto mode
	var descriptorBytes []byte
	var messageDescriptor protoreflect.MessageDescriptor
	if config.RecordType == "proto" {
		descriptorBytes, messageDescriptor, err = loadProtoDescriptor(
			config.ProtoDescriptorFile,
			config.ProtoMessageType,
		)
		if err != nil {
			sdk.Free()
			return nil, err
		}
		log.Infof("Loaded proto descriptor for message type: %s", config.ProtoMessageType)
	}

	// Configure stream options
	options := zerobus.DefaultStreamConfigurationOptions()
	if config.RecordType == "proto" {
		options.RecordType = zerobus.RecordTypeProto
	} else {
		options.RecordType = zerobus.RecordTypeJson
	}
	options.MaxInflightRequests = config.SDKOptions.MaxInflightRequests
	options.Recovery = config.SDKOptions.Recovery
	options.RecoveryRetries = config.SDKOptions.RecoveryRetries
	options.RecoveryTimeoutMs = config.SDKOptions.RecoveryTimeoutMs
	options.RecoveryBackoffMs = config.SDKOptions.RecoveryBackoffMs
	options.FlushTimeoutMs = config.SDKOptions.FlushTimeoutMs
	options.ServerLackOfAckTimeoutMs = config.SDKOptions.ServerLackOfAckTimeoutMs

	// Create table properties
	tableProps := zerobus.TableProperties{
		TableName: config.TableName,
	}
	if config.RecordType == "proto" {
		tableProps.DescriptorProto = descriptorBytes
	}

	// Create long-lived stream
	stream, err := sdk.CreateStream(
		tableProps,
		config.OAuth.ClientID,
		config.OAuth.ClientSecret,
		options,
	)
	if err != nil {
		sdk.Free()
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	// Create worker semaphore
	workerSem := make(chan struct{}, config.Workers)

	output := &zerobusOutput{
		log:               log,
		config:            config,
		codec:             codec,
		observer:          observer,
		index:             beat.Beat,
		sdk:               sdk,
		stream:            stream,
		messageDescriptor: messageDescriptor,
		workerSem:         workerSem,
	}

	log.Infof("Initialized Zerobus output for table: %s (%d workers, mode: %s)",
		config.TableName, config.Workers, config.RecordType)

	return output, nil
}

// Close implements the outputs.Client interface
func (o *zerobusOutput) Close() error {
	o.log.Info("Closing Zerobus output")

	// Close stream (flushes and waits for pending acks)
	if err := o.stream.Close(); err != nil {
		o.log.Errorf("Error closing stream: %v", err)
	}

	// Free SDK resources
	o.sdk.Free()

	return nil
}

// Publish implements the outputs.Client interface
func (o *zerobusOutput) Publish(ctx context.Context, batch publisher.Batch) error {
	events := batch.Events()
	o.observer.NewBatch(len(events))

	if len(events) == 0 {
		batch.ACK()
		return nil
	}

	// Phase 1: Pre-encode all events (codec is not thread-safe)
	type encodedEvent struct {
		index int
		data  []byte
		err   error
	}

	encodedEvents := make([]encodedEvent, len(events))
	for i := range events {
		data, err := o.codec.Encode(o.index, &events[i].Content)
		encodedEvents[i] = encodedEvent{
			index: i,
			data:  data,
			err:   err,
		}
		if err != nil {
			o.log.Errorf("Failed to encode event %d: %v", i, err)
			o.observer.WriteError(err)
		}
	}

	// Phase 2: Parallel ingestion with worker pool
	type result struct {
		index   int
		success bool
		err     error
	}

	results := make(chan result, len(events))
	var wg sync.WaitGroup

	for _, encoded := range encodedEvents {
		if encoded.err != nil {
			// Skip events that failed encoding
			results <- result{index: encoded.index, success: false, err: encoded.err}
			continue
		}

		// Acquire worker slot
		o.workerSem <- struct{}{}
		wg.Add(1)

		go func(idx int, jsonBytes []byte) {
			defer func() {
				<-o.workerSem // Release worker
				wg.Done()
			}()

			// IngestRecord blocks until queued (SDK handles backpressure)
			ack, err := o.stream.IngestRecord(string(jsonBytes))
			if err != nil {
				o.log.Errorf("Failed to ingest event %d: %v", idx, err)
				o.observer.WriteError(err)
				results <- result{index: idx, success: false, err: err}
				return
			}

			// Wait for server acknowledgment
			offset, err := ack.Await()
			if err != nil {
				o.log.Errorf("Failed to await ack for event %d: %v", idx, err)
				o.observer.WriteError(err)
				results <- result{index: idx, success: false, err: err}
				return
			}

			o.log.Debugf("Event %d acknowledged at offset %d", idx, offset)
			results <- result{index: idx, success: true}
		}(encoded.index, encoded.data)
	}

	// Wait for all workers to complete
	go func() {
		wg.Wait()
		close(results)
	}()

	// Phase 3: Collect results
	acked := 0
	failed := 0
	for r := range results {
		if r.success {
			acked++
			o.observer.WriteBytes(len(encodedEvents[r.index].data))
		} else {
			failed++
		}
	}

	// ACK the batch (always ACK to Beats)
	batch.ACK()

	// Report metrics
	o.observer.AckedEvents(acked)
	if failed > 0 {
		// Mark as permanent errors → triggers Beats retry → eventual DLQ
		o.observer.PermanentErrors(failed)
	}

	return nil
}

// String implements the outputs.Client interface
func (o *zerobusOutput) String() string {
	return fmt.Sprintf("zerobus(%s)", o.config.TableName)
}

// loadProtoDescriptor loads and parses a proto descriptor file
func loadProtoDescriptor(descriptorPath, messageType string) ([]byte, protoreflect.MessageDescriptor, error) {
	// Load descriptor file
	descriptorBytes, err := os.ReadFile(descriptorPath)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to load proto descriptor: %w", err)
	}

	// Parse descriptor
	fileDescSet := &descriptorpb.FileDescriptorSet{}
	if err := proto.Unmarshal(descriptorBytes, fileDescSet); err != nil {
		return nil, nil, fmt.Errorf("failed to parse descriptor: %w", err)
	}

	// Create file registry
	files := &protoregistry.Files{}
	for _, fdProto := range fileDescSet.File {
		fd, err := protodesc.NewFile(fdProto, files)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create file descriptor: %w", err)
		}
		if err := files.RegisterFile(fd); err != nil {
			return nil, nil, fmt.Errorf("failed to register file descriptor: %w", err)
		}
	}

	// Find message descriptor
	messageDescriptor, err := findMessageDescriptor(files, messageType)
	if err != nil {
		return nil, nil, fmt.Errorf("message type %s not found: %w", messageType, err)
	}

	return descriptorBytes, messageDescriptor, nil
}

// findMessageDescriptor searches for a message descriptor by name
func findMessageDescriptor(files *protoregistry.Files, messageType string) (protoreflect.MessageDescriptor, error) {
	var found protoreflect.MessageDescriptor

	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		messages := fd.Messages()
		for i := 0; i < messages.Len(); i++ {
			md := messages.Get(i)
			if string(md.FullName()) == messageType || string(md.Name()) == messageType {
				found = md
				return false // Stop iteration
			}
		}
		return true // Continue iteration
	})

	if found == nil {
		return nil, fmt.Errorf("message type %s not found in descriptor", messageType)
	}

	return found, nil
}
