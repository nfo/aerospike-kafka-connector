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
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class TopicConfig extends AbstractConfig {

    public static final String NAMESPACE_CONFIG = "namespace";
    private static final String NAMESPACE_DOC = "Namespace to use for the topic";

    public static final String SET_CONFIG = "set";
    private static final String SET_DOC = "Set to use for the topic";
    
    public static final String KEY_FIELD_CONFIG = "key_field";
    private static final String KEY_FIELD_DOC = "Name of the Kafka record field that contains the Aerospike user key";

    public static final String SET_FIELD_CONFIG = "set_field";
    private static final String SET_FIELD_DOC = "Name of the Kafka record field that contains the Aerospike set name";

    public static final String BINS_CONFIG = "bins";
    private static final String BINS_DOC = "Comma separated listed of bin names to include in the Aerospike record with " +
            "optinal field name mappings in the Kafka record: \"<bin1>[:<field1>][,<bin2>[:<field2>]]+\"";

    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(NAMESPACE_CONFIG, Type.STRING, Importance.LOW, NAMESPACE_DOC)
                .define(SET_CONFIG, Type.STRING, null, Importance.LOW, SET_DOC)
                .define(KEY_FIELD_CONFIG, Type.STRING, null, Importance.LOW, KEY_FIELD_DOC)
                .define(SET_FIELD_CONFIG, Type.STRING, null, Importance.LOW, SET_FIELD_DOC)
                .define(BINS_CONFIG, Type.STRING, null, Importance.LOW, BINS_DOC);
    }

    public static ConfigDef config = baseConfigDef();
    
    private final Map<String, String> binMapping;

    public TopicConfig(Map<String, Object> props) {
        super(config, props);
        binMapping = createBinMapping();
    }

    public String getNamespace() {
        return getString(NAMESPACE_CONFIG);
    }

    public String getSet() {
        return getString(SET_CONFIG);
    }
    
    public String getKeyField() {
        return getString(KEY_FIELD_CONFIG);
    }

    public String getSetField() {
        return getString(SET_FIELD_CONFIG);
    }
    
    public Map<String, String> getBinMapping() {
        return binMapping;
    }
    
    private Map<String, String> createBinMapping() {
        String binsStr = getString(BINS_CONFIG);
        if (binsStr == null) {
            return null;
        }
        Map<String, String> mapping = new HashMap<>();
        String[] list = binsStr.split(",");
        for (String entry : list) {
            String[] bin = entry.split(":", 2);
            if (bin.length == 1) {
                mapping.put(bin[0], bin[0]);
            } else {
                mapping.put(bin[1], bin[0]);
            }
        }
        return mapping;
    }
}