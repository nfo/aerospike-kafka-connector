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

import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

import com.aerospike.kafka.connect.sink.TopicConfig;

public interface RecordMapper {
	
	public void setTopicConfigs(Map<String, TopicConfig> configs);
	
	public KeyAndBins convertRecord(SinkRecord record) throws MappingError;

}