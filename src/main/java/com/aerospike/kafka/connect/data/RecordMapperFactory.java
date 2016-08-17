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
package com.aerospike.kafka.connect.data;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.aerospike.kafka.connect.sink.TopicConfig;

public class RecordMapperFactory {

	private final ConverterConfig config;
	private final Map<String, TopicConfig> topicConfigs;
	private final Map<Type, RecordConverter> instances;
	
	public RecordMapperFactory(ConverterConfig config, Map<String, TopicConfig> topicConfigs) {
		this.config = config;
		this.topicConfigs = topicConfigs;
		instances = new HashMap<>();
	}

	public RecordConverter getMapper(SinkRecord record) {
		RecordConverter mapper;
		Type type;
		Schema schema = record.valueSchema();
		if (schema != null) {
			type = schema.type();
		} else {
			Object value = record.value();
			type = ConnectSchema.schemaType(value.getClass());
		}
		if (instances.containsKey(type)) {
			mapper = instances.get(type);
		} else {
			mapper = createMapper(type);
			instances.put(type, mapper);
		}
		return mapper;
	}
	
	private RecordConverter createMapper(Type recordType) {
		switch(recordType) {
		case STRUCT:
			return new StructConverter(config, topicConfigs);
		case MAP:
			return new MapConverter(config, topicConfigs);
		default:
			throw new DataException("No mapper for records of type " + recordType);
		}
	}

}
