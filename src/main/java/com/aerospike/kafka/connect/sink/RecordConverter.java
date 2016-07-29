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
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.kafka.connect.errors.ConversionError;

public class RecordConverter {

	private static final String DEFAULT_KEY = "value"; 
	private static final Logger log = LoggerFactory.getLogger(RecordConverter.class);

	public KeyAndBins convertRecord(SinkRecord record, String namespace, String set) throws ConversionError {
		Key key = keyFromRecord(record, namespace, set);
		Bin[] bins = binsFromRecord(record);
		return new KeyAndBins(key, bins);
	}

	private Key keyFromRecord(SinkRecord record, String namespace, String set) throws ConversionError {
		Object key = record.key();
		if (key instanceof String) {
			return new Key(namespace, set, (String)key);
		} else {
			throw new ConversionError(String.format("Unsupported key: {}", key));
		}
	}

	private Bin[] binsFromRecord(SinkRecord record) throws ConversionError {
		Bin[] bins;
		Object value = record.value();
		Schema schema = record.valueSchema();
		if (schema != null) {
			bins = binsFromValueWithSchema(value, schema);
		} else {
			bins = binsFromValue(value);
		}
		return bins;
	}
	
	private Bin[] binsFromValue(Object value) {
		List<Bin> bins = new ArrayList<Bin>();
		if (value instanceof Map) {
			Map<?, ?> map = (Map<?, ?>)value;
			for (Map.Entry<?, ?>entry : map.entrySet()) {
				bins.add(new Bin(entry.getKey().toString(), entry.getValue()));
			}
		} else {
			bins.add(new Bin(DEFAULT_KEY, value));
		}
		return bins.toArray(new Bin[0]);
	}

	private Bin[] binsFromValueWithSchema(Object value, Schema schema) throws ConversionError {
		Bin[] bins;
		if (schema.type() == Type.STRUCT) {
			bins = binsFromStruct((Struct)value);
		} else {
			bins = new Bin[] { new Bin(DEFAULT_KEY, value) };
		}
		return bins;
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
