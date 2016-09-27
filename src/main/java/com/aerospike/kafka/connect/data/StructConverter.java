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
import java.util.HashMap;
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

    public StructConverter(Map<String, TopicConfig> topicConfigs) {
        super(topicConfigs);
    }

    public AerospikeRecord convertRecord(SinkRecord record) {
        Struct value = asStruct(record.value(), record.valueSchema());
        TopicConfig topicConfig = getTopicConfig(record);
        Key key = keyFromRecord(value, record.key(), topicConfig);
        Bin[] bins = binsFromStruct(value, topicConfig);
        return new AerospikeRecord(key, bins);
    }

    private Struct asStruct(Object value, Schema schema) {
        if (schema == null) {
            throw new DataException("Missing record schema");
        }
        if (schema.type() != Type.STRUCT) {
            throw new DataException("Unsupported schema type - expected Struct, got " + schema.type());
        }
        return (Struct) value;
    }

    private Key keyFromRecord(Struct struct, Object recordKey, TopicConfig config) {
        String namespace = config.getNamespace();
        String set = config.getSet();
        Object userKey = recordKey;
        String setField = config.getSetField();
        if (setField != null) {
            set = struct.getString(setField);
        }
        String keyField = config.getKeyField();
        if (keyField != null) {
            userKey = struct.get(keyField);
        }
        Key key = createKey(namespace, set, userKey);
        return key;
    }

    private Key createKey(String namespace, String set, Object userKey) {
        Value userKeyValue = Value.get(userKey);
        return new Key(namespace, set, userKeyValue);
    }

    private Bin[] binsFromStruct(Struct struct, TopicConfig config) {
        Map<String, String> binMapping = config.getBinMapping();
        List<Field> fields = struct.schema().fields();
        List<Bin> bins = new ArrayList<Bin>();
        for (Field field : fields) {
            Bin bin = binFromField(struct, field, binMapping);
            if (bin != null) {
                bins.add(bin);
            }
        }
        return bins.toArray(new Bin[0]);
    }

    private Bin binFromField(Struct struct, Field field, Map<String, String> binMapping) {
        String fieldName = field.name();
        String binName = fieldName;
        if (binMapping != null) {
            binName = binMapping.get(fieldName); 
            if (binName == null) {
                return null;
            }
        }

        Bin bin = null;
        Type type = field.schema().type();
        switch (type) {
        case ARRAY:
            bin = new Bin(binName, struct.getArray(fieldName));
            break;
        case BOOLEAN:
            bin = new Bin(binName, struct.getBoolean(fieldName));
            break;
        case BYTES:
            bin = new Bin(binName, struct.getBytes(fieldName));
            break;
        case FLOAT32:
            bin = new Bin(binName, struct.getFloat32(fieldName));
            break;
        case FLOAT64:
            bin = new Bin(binName, struct.getFloat64(fieldName));
            break;
        case INT8:
            bin = new Bin(binName, struct.getInt8(fieldName));
            break;
        case INT16:
            bin = new Bin(binName, struct.getInt16(fieldName));
            break;
        case INT32:
            bin = new Bin(binName, struct.getInt32(fieldName));
            break;
        case INT64:
            bin = new Bin(binName, struct.getInt64(fieldName));
            break;
        case MAP:
            bin = new Bin(binName, struct.getMap(fieldName));
            break;
        case STRING:
            bin = new Bin(binName, struct.getString(fieldName));
            break;
        case STRUCT:
            Struct nestedStruct = struct.getStruct(fieldName);
            bin = new Bin(binName, mapFromStruct(nestedStruct));
        default:
            log.info("Ignoring struct field {} of unsupported type {}", fieldName, type);
        }
        return bin;
    }
    
    private Map<String, Object> mapFromStruct(Struct struct) {
        List<Field> fields = struct.schema().fields();
        Map<String, Object> map = new HashMap<>();
        for (Field field : fields) {
            String name = field.name();
            Type type = field.schema().type();
            switch (type) {
            case STRUCT:
                Struct nestedStruct = struct.getStruct(name);
                map.put(name, mapFromStruct(nestedStruct));
                break;
            default:
                map.put(name, struct.get(name));
            }
        }
        return map;
    }
}