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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import com.aerospike.kafka.connect.sink.TopicConfig;

public class StructConverterTest extends AbstractConverterTest {

	@Override
	public RecordConverter getConverter(ConverterConfig config, Map<String, TopicConfig> topicConfigs) {
		return new StructConverter(config, topicConfigs);
	}

	public SinkRecord createSinkRecord(String topic, Object key, Object ...keysAndValues) {
		Schema keySchema = null;
		if (key != null) {
			keySchema = buildObjectSchema(key);
		}
		Schema recordSchema = buildRecordSchema(keysAndValues);
		Struct struct = new Struct(recordSchema);
		for (int i = 0; i < keysAndValues.length; i = i + 2) {
			String fieldName = (String)keysAndValues[i];
			Object fieldValue = keysAndValues[i+1];
			struct.put(fieldName, fieldValue);
		}
		int partition = 0;
		long offset = 0;
		return new SinkRecord(topic, partition, keySchema, key, recordSchema, struct, offset);
	}
	
	private Schema buildRecordSchema(Object ...keysAndValues) {
		SchemaBuilder builder = SchemaBuilder.struct();
		for (int i = 0; i < keysAndValues.length; i = i + 2) {
			String fieldName = (String)keysAndValues[i];
			Schema fieldSchema = buildObjectSchema(keysAndValues[i+1]);
			builder.field(fieldName, fieldSchema);
		}
		return builder.build();
		
	}

	private Schema buildObjectSchema(Object value) {
		Schema objectSchema;
		Type type = ConnectSchema.schemaType(value.getClass());
		switch(type){
		case MAP:
			Map<?, ?> map = (Map<?, ?>)value;
			Entry<?, ?> mapEntry = map.entrySet().iterator().next();
			objectSchema = SchemaBuilder.map(
					buildObjectSchema(mapEntry.getKey()),
					buildObjectSchema(mapEntry.getValue()));
			break;
		case ARRAY:
			List<?> list = (List<?>)value;
			Object listEntry = list.iterator().next();
			objectSchema = SchemaBuilder.array(
					buildObjectSchema(listEntry));
			break;
		default:
			objectSchema = SchemaBuilder.type(type).build();
		}
		return objectSchema;
	}
}