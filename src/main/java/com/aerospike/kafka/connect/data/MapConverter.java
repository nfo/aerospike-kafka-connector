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

import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.kafka.connect.sink.TopicConfig;

public class MapConverter extends RecordConverter {

	public MapConverter(ConverterConfig config, Map<String, TopicConfig> topicConfigs) {
		super(config, topicConfigs);
	}

	public AerospikeRecord convertRecord(SinkRecord record) {
		Map<?, ?> value = asMap(record.value());
		TopicConfig topicConfig = getTopicConfig(record);
		Key key = keyFromRecord(value, record.key(), topicConfig);
		Bin[] bins = binsFromMap(value);
		return new AerospikeRecord(key, bins);
	}
	
	private Map<?, ?> asMap(Object value) {
		if (value instanceof Map) {
			return (Map<?, ?>) value;
		}
		throw new DataException("Unsupported record type - expected to get map instance");
	}
	
	private Key keyFromRecord(Map<?, ?> recordMap, Object recordKey, TopicConfig topicConfig) {
		String namespace = topicConfig.getNamespace();
		String set = topicConfig.getSet();
		Object userKey = recordKey;
		ConverterConfig config = getConfig();
		String setField = config.getSetField();
		if (setField != null) {
			if (!recordMap.containsKey(setField)) {
				throw new DataException("Record is missing " + setField + " field - cannot determine Set name.");
			}
			set = recordMap.get(setField).toString();
		}
		String keyField = config.getKeyField();
		if (keyField != null) {
			if (!recordMap.containsKey(keyField)) {
				throw new DataException("Record is missing " + keyField + " field - cannot determine Key value.");
			}
			userKey = recordMap.get(keyField);
		}
		Key key = createKey(namespace, set, userKey);
		return key;
	}

	private Key createKey(String namespace, String set, Object userKey) {
		Value userKeyValue = Value.get(userKey);
		return new Key(namespace, set, userKeyValue);
	}

	private Bin[] binsFromMap(Map<?, ?> map) {
		List<Bin> bins = new ArrayList<Bin>();
		for (Map.Entry<?, ?>entry : map.entrySet()) {
			bins.add(new Bin(entry.getKey().toString(), entry.getValue()));
		}
		return bins.toArray(new Bin[0]);
	}
}