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
package com.aerospike.kafka.connect.mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class JsonObjectMapper extends AbstractRecordMapper {

	public KeyAndBins convertRecord(SinkRecord record) throws MappingError {
		Key key = keyFromRecord(record);
		Bin[] bins = binsFromRecord(record);
		return new KeyAndBins(key, bins);
	}

	private Bin[] binsFromRecord(SinkRecord record) throws MappingError {
		Object value = record.value();
		if (!(value instanceof Map)) {
			throw new MappingError("Unsupported record type");
		}
		return binsFromMap((Map<?, ?>)value);
	}
	
	private Bin[] binsFromMap(Map<?, ?> map) {
		List<Bin> bins = new ArrayList<Bin>();
		for (Map.Entry<?, ?>entry : map.entrySet()) {
			bins.add(new Bin(entry.getKey().toString(), entry.getValue()));
		}
		return bins.toArray(new Bin[0]);
	}
}