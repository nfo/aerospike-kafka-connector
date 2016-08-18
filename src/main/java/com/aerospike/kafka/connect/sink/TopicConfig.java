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

    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(NAMESPACE_CONFIG, Type.STRING, Importance.LOW, NAMESPACE_DOC)
                .define(SET_CONFIG, Type.STRING, Importance.LOW, SET_DOC);
    }

    public static ConfigDef config = baseConfigDef();

    public TopicConfig(Map<String, Object> props) {
        super(config, props);
    }

    public String getNamespace() {
        return getString(NAMESPACE_CONFIG);
    }

    public String getSet() {
        return getString(SET_CONFIG);
    }
}