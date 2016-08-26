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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.kafka.connect.data.RecordConverter;
import com.aerospike.kafka.connect.data.AerospikeRecord;
import com.aerospike.kafka.connect.sink.TopicConfig;

public abstract class AbstractConverterTest {

    public abstract RecordConverter getConverter(Map<String, TopicConfig> topicConfigs);

    public abstract SinkRecord createSinkRecord(String topic, Object key, Object... keysAndValues);

    @Test
    public void testConvertStringBin() {
        Map<String, TopicConfig> config = configFor("testTopic", 
                "namespace", "topicNamespace", 
                "set", "topicSet"
                );
        RecordConverter subject = getConverter(config);
        SinkRecord record = createSinkRecord("testTopic", "testKey", "sBin", "aString");

        AerospikeRecord result = subject.convertRecord(record);

        Bin bins[] = result.bins();
        assertEquals(1, bins.length);
        Bin bin = bins[0];
        assertEquals("sBin", bin.name);
        assertEquals("aString", bin.value.toString());
    }

    @Test
    public void testConvertIntegerBin() {
        Map<String, TopicConfig> config = configFor("testTopic", 
                "namespace", "topicNamespace", 
                "set", "topicSet"
                );
        RecordConverter subject = getConverter(config);
        SinkRecord record = createSinkRecord("testTopic", "testKey", "iBin", 12345);

        AerospikeRecord result = subject.convertRecord(record);

        Bin bins[] = result.bins();
        assertEquals(1, bins.length);
        Bin bin = bins[0];
        assertEquals("iBin", bin.name);
        assertEquals(12345, bin.value.toInteger());
    }

    @Test
    public void testConvertDoubleBin() {
        Map<String, TopicConfig> config = configFor("testTopic", 
                "namespace", "topicNamespace", 
                "set", "topicSet"
                );
        RecordConverter subject = getConverter(config);
        SinkRecord record = createSinkRecord("testTopic", "testKey", "dBin", 12.345);

        AerospikeRecord result = subject.convertRecord(record);

        Bin bins[] = result.bins();
        assertEquals(1, bins.length);
        Bin bin = bins[0];
        assertEquals("dBin", bin.name);
        assertEquals(Double.valueOf(12.345), (Double) bin.value.getObject());
    }

    @Test
    public void testConvertListBin() {
        Map<String, TopicConfig> config = configFor("testTopic", 
                "namespace", "topicNamespace", 
                "set", "topicSet"
                );
        RecordConverter subject = getConverter(config);
        SinkRecord record = createSinkRecord("testTopic", "testKey", "lBin", Arrays.asList("aString", "anotherString"));

        AerospikeRecord result = subject.convertRecord(record);

        Bin bins[] = result.bins();
        assertEquals(1, bins.length);
        Bin bin = bins[0];
        assertEquals("lBin", bin.name);
        List<?> value = (List<?>) bin.value.getObject();
        assertEquals("aString", value.get(0));
        assertEquals("anotherString", value.get(1));
    }

    @Test
    public void testConvertMapBin() {
        Map<String, TopicConfig> config = configFor("testTopic", 
                "namespace", "topicNamespace", 
                "set", "topicSet"
                );
        RecordConverter subject = getConverter(config);
        SinkRecord record = createSinkRecord("testTopic", "testKey", "mBin",
                Collections.singletonMap("aKey", "aValue"));

        AerospikeRecord result = subject.convertRecord(record);

        Bin bins[] = result.bins();
        assertEquals(1, bins.length);
        Bin bin = bins[0];
        assertEquals("mBin", bin.name);
        Map<?, ?> value = (Map<?, ?>) bin.value.getObject();
        assertEquals("aValue", value.get("aKey"));
    }

    @Test
    public void testConvertSetName() {
        Map<String, TopicConfig> config = configFor("testTopic", 
                "namespace", "topicNamespace", 
                "set_field", "bin1"
                );
        RecordConverter subject = getConverter(config);
        SinkRecord record = createSinkRecord("testTopic", "testKey", "bin1", "aString", "bin2", 12345);

        AerospikeRecord result = subject.convertRecord(record);

        Key askey = result.key();
        assertEquals("topicNamespace", askey.namespace);
        assertEquals("aString", askey.setName);
        assertEquals("testKey", askey.userKey.toString());
    }

    @Test
    public void testConvertStringKey() {
        Map<String, TopicConfig> config = configFor("testTopic", 
                "namespace", "topicNamespace", 
                "set", "topicSet",
                "key_field", "bin1"
                );
        RecordConverter subject = getConverter(config);
        SinkRecord record = createSinkRecord("testTopic", "testKey", "bin1", "aString", "bin2", 12345);

        AerospikeRecord result = subject.convertRecord(record);

        Key askey = result.key();
        assertEquals("topicNamespace", askey.namespace);
        assertEquals("topicSet", askey.setName);
        assertEquals("aString", askey.userKey.toString());
    }

    @Test
    public void testConvertIntegerKey() {
        Map<String, TopicConfig> config = configFor("testTopic", 
                "namespace", "topicNamespace", 
                "set", "topicSet",
                "key_field", "bin2"
                );
        RecordConverter subject = getConverter(config);
        SinkRecord record = createSinkRecord("testTopic", "testKey", "bin1", "aString", "bin2", 12345);

        AerospikeRecord result = subject.convertRecord(record);

        Key askey = result.key();
        assertEquals("topicNamespace", askey.namespace);
        assertEquals("topicSet", askey.setName);
        assertEquals(12345, askey.userKey.toInteger());
    }

    @Test
    public void testConvertLongKey() {
        Map<String, TopicConfig> config = configFor("testTopic", 
                "namespace", "topicNamespace", 
                "set", "topicSet",
                "key_field", "bin2"
                );
        RecordConverter subject = getConverter(config);
        SinkRecord record = createSinkRecord("testTopic", "testKey", "bin1", "aString", "bin2", 12345678l);

        AerospikeRecord result = subject.convertRecord(record);

        Key askey = result.key();
        assertEquals("topicNamespace", askey.namespace);
        assertEquals("topicSet", askey.setName);
        assertEquals(12345678l, askey.userKey.toLong());
    }

    @Test
    public void testConvertBytesKey() {
        Map<String, TopicConfig> config = configFor("testTopic", 
                "namespace", "topicNamespace", 
                "set", "topicSet",
                "key_field", "bin2"
                );
        RecordConverter subject = getConverter(config);
        SinkRecord record = createSinkRecord("testTopic", "testKey", "bin1", "aString", "bin2",
                new byte[] { 0x01, 0x02, 0x03, 0x04 });

        AerospikeRecord result = subject.convertRecord(record);

        Key askey = result.key();
        assertEquals("topicNamespace", askey.namespace);
        assertEquals("topicSet", askey.setName);
        assertArrayEquals(new byte[] { 0x01, 0x02, 0x03, 0x04 }, (byte[]) askey.userKey.getObject());
    }

    protected Map<String, TopicConfig> configFor(String topic, String... keysAndValues) {
        Map<String, Object> config = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i = i + 2) {
            config.put(keysAndValues[i], keysAndValues[i + 1]);
        }
        return Collections.singletonMap(topic, new TopicConfig(config));
    }

}