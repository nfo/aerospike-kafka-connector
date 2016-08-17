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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.kafka.connect.sink.TopicConfig;

public class StructConverter extends RecordConverter {

	private static final Logger log = LoggerFactory.getLogger(StructConverter.class);

	public StructConverter(ConverterConfig config, Map<String, TopicConfig> topicConfigs) {
		super(config, topicConfigs);
	}
	
	public AerospikeRecord convertRecord(SinkRecord record) {
		Struct value = asStruct(record.value(), record.valueSchema());
		TopicConfig topicConfig = getTopicConfig(record);
		Key key = keyFromRecord(value, record.key(), topicConfig);
		Bin[] bins = binsFromStruct(value);
		return new AerospikeRecord(key, bins);
	}

	private Struct asStruct(Object value, Schema schema) {
		if (schema == null) {
			throw new DataException("Missing record schema");
		}
		if (schema.type() != Type.STRUCT) {
			throw new DataException("Unsupported schema type - expected Struct, got " + schema.type());
		}
		return (Struct)value;
	}

	private Key keyFromRecord(Struct struct, Object recordKey, TopicConfig topicConfig) {
		String namespace = topicConfig.getNamespace();
		String set = topicConfig.getSet();
		Object userKey = recordKey;
		ConverterConfig config = getConfig();
		if (config.getSetField() != null) {
			set = struct.getString(config.getSetField());
		}
		if (config.getKeyField() != null) {
			userKey = struct.get(config.getKeyField());
		}
		Key key = createKey(namespace, set, userKey);
		return key;
	}

	private Key createKey(String namespace, String set, Object userKey) {
		Value userKeyValue = Value.get(userKey);
		return new Key(namespace, set, userKeyValue);
	}

	private Bin[] binsFromStruct(Struct struct) {
		List<Field> fields = struct.schema().fields();
		List<Bin> bins = new ArrayList<Bin>();
		for (Field field : fields) {
			String name = field.name();
			Type type = field.schema().type();
			switch(type) {
			case ARRAY:
				bins.add(new Bin(name, struct.getArray(name)));
				break;
			case BOOLEAN:
				bins.add(new Bin(name, struct.getBoolean(name)));
				break;
			case BYTES:
				bins.add(new Bin(name, struct.getBytes(name)));
				break;
			case FLOAT32:
				bins.add(new Bin(name, struct.getFloat32(name)));
				break;
			case FLOAT64:
				bins.add(new Bin(name, struct.getFloat64(name)));
				break;
			case INT8:
				bins.add(new Bin(name, struct.getInt8(name)));
				break;
			case INT16:
				bins.add(new Bin(name, struct.getInt16(name)));
				break;
			case INT32:
				bins.add(new Bin(name, struct.getInt32(name)));
				break;
			case INT64:
				bins.add(new Bin(name, struct.getInt64(name)));
				break;
			case MAP:
				bins.add(new Bin(name, struct.getMap(name)));
				break;
			case STRING:
				bins.add(new Bin(name, struct.getString(name)));
				break;
			case STRUCT: // TODO: handle nested struct values
			default:
				log.debug("Ignoring struct field {} of unsupported type {}", name, type);
			}
		}
		return bins.toArray(new Bin[0]);
	}
}