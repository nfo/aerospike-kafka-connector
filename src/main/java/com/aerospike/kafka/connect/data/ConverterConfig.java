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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class ConverterConfig extends AbstractConfig {

	public static final String KEY_FIELD_CONFIG = "key_field";
	private static final String KEY_FIELD_DOC = "Name of the Kafka record field that contains the Aerospike user key";

	public static final String SET_FIELD_CONFIG = "set_field";
	private static final String SET_FIELD_DOC = "Name of the Kafka record field that contains the Aerospike set name";
	
	public static ConfigDef baseConfigDef() {
		return new ConfigDef()
				.define(KEY_FIELD_CONFIG, Type.STRING, null, Importance.MEDIUM, KEY_FIELD_DOC)
				.define(SET_FIELD_CONFIG, Type.STRING, null, Importance.MEDIUM, SET_FIELD_DOC);
	}
	
	public static ConfigDef config = baseConfigDef();
	
	public ConverterConfig(Map<String, ?> props) {
		super(config, props);
	}
	
	public String getKeyField() {
		return getString(KEY_FIELD_CONFIG);
	}
	
	public String getSetField() {
		return getString(SET_FIELD_CONFIG);
	}
}