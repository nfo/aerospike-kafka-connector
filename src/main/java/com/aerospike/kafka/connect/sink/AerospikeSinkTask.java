/*
 * Copyright 2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.kafka.connect.sink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.kafka.connect.mapper.KeyAndBins;
import com.aerospike.kafka.connect.mapper.MappingError;
import com.aerospike.kafka.connect.mapper.RecordMapper;

public class AerospikeSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(AerospikeSinkTask.class);
	
	private RecordMapper mapper;
	private AerospikeClient client;
	private WritePolicy writePolicy;

	public AerospikeSinkTask() {
	}

	public String version() {
		return new AerospikeSinkConnector().version();
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		// TODO Auto-generated method stub

	}

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
		for (SinkRecord record : sinkRecords) {
			try {
				KeyAndBins keyAndBins = mapper.convertRecord(record);
				Key key = keyAndBins.getKey();
				Bin[] bins = keyAndBins.getBins();
				log.trace("Writing record for key {}: {}", key, bins);
				client.put(writePolicy, key, bins);
			} catch (MappingError e) {
				log.error("Error converting record", e);
			} catch (AerospikeException e) {
				log.error("Error writing to record", e);
			}
		}
	}

	@Override
	public void start(Map<String, String> props) {
		log.trace("Starting {} task with config: {}", this.getClass().getName(), props);
		ConnectorConfig config = new ConnectorConfig(props);
		try {
			String hostname = config.getHostname();
			int port = config.getPort();
			ClientPolicy policy = new ClientPolicy();
			client = new AerospikeClient(policy, hostname, port);
		} catch (AerospikeException e) {
			throw new ConnectException("Could not connect to Aerospike cluster", e);
		}

		writePolicy = createWritePolicy(config);
		mapper = config.getRecordMapper();
	}

	@Override
	public void stop() {
		log.trace("Stopping {} task", this.getClass().getName());
	}

	private WritePolicy createWritePolicy(ConnectorConfig config) {
		WritePolicy policy = new WritePolicy();
		RecordExistsAction action = config.getPolicyRecordExistsAction();
		if (action != null) {
			policy.recordExistsAction = action;
		}
		log.trace("Write Policy: recordExistsAction={}", policy.recordExistsAction);
		return policy;
	}

}
