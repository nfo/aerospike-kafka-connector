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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.kafka.connect.mapper.JsonObjectMapper;
import com.aerospike.kafka.connect.mapper.RecordMapper;

public class ConnectorConfig extends AbstractConfig {
	
	private static final Logger log = LoggerFactory.getLogger(ConnectorConfig.class);

	public static final String TOPICS_CONFIG = AerospikeSinkConnector.TOPICS_CONFIG;
	private static final String TOPICS_DOC = "List of Kafka topics";

	public static final String HOSTNAME_CONFIG = "hostname";
	private static final String HOSTNAME_DOC = "Aerospike Hostname";

	public static final String PORT_CONFIG = "port";
	private static final String PORT_DOC = "Aerospike Port";
	private static final int PORT_DEFAULT = 3000;
	private static final Validator PORT_VALIDATOR = Range.between(1, 65535);

	public static final String POLICY_RECORD_EXISTS_ACTION_CONFIG = "policy.record_exists_action";
	private static final String POLICY_RECORD_EXISTS_ACTION_DOC = "Write Policy: How to handle writes when the record already exists";
	private static final String POLICY_RECORD_EXISTS_ACTION_DEFAULT = "update";
	private static final Validator POLICY_RECORD_EXISTS_ACTION_VALIDATOR = ValidString.in("create_only", "update", "update_only", "replace", "replace_only");
	
	public static final String MAPPER_CONFIG = "mapper";
	private static final String MAPPER_DOC = "Record mapper class";
	private static final Class<? extends RecordMapper> MAPPER_DEFAULT = JsonObjectMapper.class;

	public static ConfigDef baseConfigDef() {
		return new ConfigDef()
				.define(TOPICS_CONFIG, Type.LIST, Importance.HIGH, TOPICS_DOC)
				.define(HOSTNAME_CONFIG, Type.STRING, Importance.HIGH, HOSTNAME_DOC)
				.define(PORT_CONFIG, Type.INT, PORT_DEFAULT, PORT_VALIDATOR, Importance.LOW, PORT_DOC)
				.define(POLICY_RECORD_EXISTS_ACTION_CONFIG, Type.STRING, POLICY_RECORD_EXISTS_ACTION_DEFAULT, POLICY_RECORD_EXISTS_ACTION_VALIDATOR, Importance.LOW, POLICY_RECORD_EXISTS_ACTION_DOC)
				.define(MAPPER_CONFIG, Type.CLASS, MAPPER_DEFAULT, Importance.HIGH, MAPPER_DOC);
	}

	static ConfigDef config = baseConfigDef();

	public ConnectorConfig(Map<String, String> props) {
		super(config, props);
	}
	
	public String getHostname() {
		return getString(HOSTNAME_CONFIG);
	}
	
	public int getPort() {
		return getInt(PORT_CONFIG);
	}
	
	public RecordExistsAction getPolicyRecordExistsAction() {
		String action = getString(POLICY_RECORD_EXISTS_ACTION_CONFIG);
		switch(action) {
		case "create_only":
			return RecordExistsAction.CREATE_ONLY;
		case "replace":
			return RecordExistsAction.REPLACE;
		case "replace_only":
			return RecordExistsAction.REPLACE_ONLY;
		case "update":
			return RecordExistsAction.UPDATE;
		case "update_only":
			return RecordExistsAction.UPDATE_ONLY;
		default:
			// This should never happen if the configuration passes validation!
			throw new ConfigException(POLICY_RECORD_EXISTS_ACTION_CONFIG, action, "Unsupported policy value.");
		}
	}
	
	public TopicConfig getTopicConfig(String prefix) {
		return getTopicConfig(prefix, null);
	}

	public TopicConfig getTopicConfig(String prefix, TopicConfig defaultConfig) {
		Map<String, String> props = new HashMap<>();
		if (defaultConfig != null) {
			props.putAll(defaultConfig.originalsStrings());
		}
		for (Map.Entry<String, Object> entry : originalsWithPrefix(prefix).entrySet()) {
			props.put(entry.getKey(), entry.getValue().toString());
		}
		log.trace("Creating topic config for prefix {}: {}", prefix, props);
		return new TopicConfig(props);
	}
	
	public Map<String, TopicConfig> getTopicConfigs() {
		Map<String, TopicConfig> topicConfigs = new HashMap<>();
		TopicConfig defaultTopicConfig = getTopicConfig("topic.");
		List<String> topicNames = getList(TOPICS_CONFIG);
		for (String topic : topicNames) {
			String prefix = "topic." + topic + ".";
			TopicConfig config = getTopicConfig(prefix, defaultTopicConfig);
			topicConfigs.put(topic, config);
		}
		return topicConfigs;
	}
	
	public RecordMapper getRecordMapper() {
		RecordMapper mapper = getConfiguredInstance("mapper", RecordMapper.class);
		mapper.setTopicConfigs(getTopicConfigs());
		return mapper;
	}

}