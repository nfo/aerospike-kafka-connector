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

import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.kafka.connect.sink.TopicConfig;

public class StructConverterTest extends AbstractConverterTest {

    @Test
    public void testConvertStructBin() {
        Map<String, TopicConfig> config = configFor("testTopic", 
                "namespace", "topicNamespace", 
                "set", "topicSet"
                );
        RecordConverter subject = getConverter(config);
        SinkRecord record = createSinkRecord("testTopic", "testKey", "structBin", buildStruct("aKey", "aValue", "intKey", 42));

        AerospikeRecord result = subject.convertRecord(record);

        Bin bins[] = result.bins();
        assertEquals(1, bins.length);
        Bin bin = bins[0];
        assertEquals("structBin", bin.name);
        Map<?, ?> value = (Map<?, ?>) bin.value.getObject();
        assertEquals("aValue", value.get("aKey"));
        assertEquals(42, value.get("intKey"));
    }

    @Override
    public RecordConverter getConverter(Map<String, TopicConfig> config) {
        return new StructConverter(config);
    }

    public SinkRecord createSinkRecord(String topic, Object key, Object... keysAndValues) {
        Schema keySchema = null;
        if (key != null) {
            keySchema = buildObjectSchema(key);
        }
        Struct struct = buildStruct(keysAndValues);
        int partition = 0;
        long offset = 0;
        return new SinkRecord(topic, partition, keySchema, key, struct.schema(), struct, offset);
    }

    private Struct buildStruct(Object... keysAndValues) {
        Schema recordSchema = buildStructSchema(keysAndValues);
        Struct struct = new Struct(recordSchema);
        for (int i = 0; i < keysAndValues.length; i = i + 2) {
            String fieldName = (String) keysAndValues[i];
            Object fieldValue = keysAndValues[i + 1];
            struct.put(fieldName, fieldValue);
        }
        return struct;
    }

    private Schema buildStructSchema(Object... keysAndValues) {
        SchemaBuilder builder = SchemaBuilder.struct();
        for (int i = 0; i < keysAndValues.length; i = i + 2) {
            String name = (String) keysAndValues[i];
            Object value = keysAndValues[i + 1];
            Schema schema = buildObjectSchema(value);
            builder.field(name, schema);
        }
        return builder.build();

    }

    private Schema buildObjectSchema(Object value) {
        if (value instanceof Struct) {
            return ((Struct) value).schema();
        }
        Schema objectSchema;
        Type type = ConnectSchema.schemaType(value.getClass());
        switch (type) {
        case MAP:
            Map<?, ?> map = (Map<?, ?>) value;
            Entry<?, ?> mapEntry = map.entrySet().iterator().next();
            objectSchema = SchemaBuilder.map(buildObjectSchema(mapEntry.getKey()),
                    buildObjectSchema(mapEntry.getValue()));
            break;
        case ARRAY:
            List<?> list = (List<?>) value;
            Object listEntry = list.iterator().next();
            objectSchema = SchemaBuilder.array(buildObjectSchema(listEntry));
            break;
        default:
            objectSchema = SchemaBuilder.type(type).build();
        }
        return objectSchema;
    }
}