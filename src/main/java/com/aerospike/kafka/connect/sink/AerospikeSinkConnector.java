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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.kafka.connect.Version;

public class AerospikeSinkConnector extends SinkConnector {

	private static final Logger log = LoggerFactory.getLogger(AerospikeSinkConnector.class);

	private Map<String, String> configProperties;

	@Override
	public ConfigDef config() {
		return ConnectorConfig.config;
	}

	@Override
	public void start(Map<String, String> props) {
		log.info("Starting {} connector", this.getClass().getName());
		try {
			configProperties = props;
			new ConnectorConfig(props);
		} catch (ConfigException e) {
			throw new ConnectException("Could not start AerospikeSinkConnector due to configuration error", e);
		}
	}

	@Override
	public void stop() {
		// Nothing to do since AerospikeSinkConnector has no background monitoring
		log.info("Stopping {} connector", this.getClass().getName());
	}

	@Override
	public Class<? extends Task> taskClass() {
		return AerospikeSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> taskConfigs = new ArrayList<>();
		Map<String, String> taskProps = new HashMap<>();
		taskProps.putAll(configProperties);
		for (int i = 0; i < maxTasks; i++) {
			taskConfigs.add(taskProps);
		}
		return taskConfigs;
	}

	@Override
	public String version() {
		return Version.getVersion();
	}
}