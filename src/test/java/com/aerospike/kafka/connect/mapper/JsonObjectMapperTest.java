package com.aerospike.kafka.connect.mapper;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.kafka.connect.sink.TopicConfig;

public class JsonObjectMapperTest {

	@Test
	public void testMapStringBin() throws MappingError {
		Map<String, TopicConfig> topicConfigs = createTopicConfigs("testTopic", "topicNamespace", "topicSet");
		Map<String, Object> config = new HashMap<>();
		JsonObjectMapper subject = new JsonObjectMapper();
		subject.configure(config);
		subject.setTopicConfigs(topicConfigs);
		SinkRecord record = createSinkRecord("testTopic", "testKey", "sBin", "aString");

		KeyAndBins result = subject.convertRecord(record);

		Bin bins[] = result.getBins();
		assertEquals(1, bins.length);
		Bin bin = bins[0];
		assertEquals("sBin", bin.name);
		assertEquals("aString", bin.value.toString());
	}

	@Test
	public void testMapIntegerBin() throws MappingError {
		Map<String, TopicConfig> topicConfigs = createTopicConfigs("testTopic", "topicNamespace", "topicSet");
		Map<String, Object> config = new HashMap<>();
		JsonObjectMapper subject = new JsonObjectMapper();
		subject.configure(config);
		subject.setTopicConfigs(topicConfigs);
		SinkRecord record = createSinkRecord("testTopic", "testKey", "iBin", 12345);

		KeyAndBins result = subject.convertRecord(record);

		Bin bins[] = result.getBins();
		assertEquals(1, bins.length);
		Bin bin = bins[0];
		assertEquals("iBin", bin.name);
		assertEquals(12345, bin.value.toInteger());
	}

	@Test
	public void testMapDoubleBin() throws MappingError {
		Map<String, TopicConfig> topicConfigs = createTopicConfigs("testTopic", "topicNamespace", "topicSet");
		Map<String, Object> config = new HashMap<>();
		JsonObjectMapper subject = new JsonObjectMapper();
		subject.configure(config);
		subject.setTopicConfigs(topicConfigs);
		SinkRecord record = createSinkRecord("testTopic", "testKey", "dBin", 12.345);

		KeyAndBins result = subject.convertRecord(record);

		Bin bins[] = result.getBins();
		assertEquals(1, bins.length);
		Bin bin = bins[0];
		assertEquals("dBin", bin.name);
		assertEquals(Double.valueOf(12.345), (Double)bin.value.getObject());
	}

	@Test
	public void testMapListBin() throws MappingError {
		Map<String, TopicConfig> topicConfigs = createTopicConfigs("testTopic", "topicNamespace", "topicSet");
		Map<String, Object> config = new HashMap<>();
		JsonObjectMapper subject = new JsonObjectMapper();
		subject.configure(config);
		subject.setTopicConfigs(topicConfigs);
		SinkRecord record = createSinkRecord("testTopic", "testKey", "lBin", Arrays.asList("aString", "anotherString"));

		KeyAndBins result = subject.convertRecord(record);

		Bin bins[] = result.getBins();
		assertEquals(1, bins.length);
		Bin bin = bins[0];
		assertEquals("lBin", bin.name);
		List<?> value = (List<?>)bin.value.getObject();
		assertEquals("aString", value.get(0));
		assertEquals("anotherString", value.get(1));
	}

	@Test
	public void testMapMapBin() throws MappingError {
		Map<String, TopicConfig> topicConfigs = createTopicConfigs("testTopic", "topicNamespace", "topicSet");
		Map<String, Object> config = new HashMap<>();
		JsonObjectMapper subject = new JsonObjectMapper();
		subject.configure(config);
		subject.setTopicConfigs(topicConfigs);
		SinkRecord record = createSinkRecord("testTopic", "testKey", "mBin", Collections.singletonMap("aKey", "aValue"));

		KeyAndBins result = subject.convertRecord(record);

		Bin bins[] = result.getBins();
		assertEquals(1, bins.length);
		Bin bin = bins[0];
		assertEquals("mBin", bin.name);
		Map<?, ?> value = (Map<?, ?>)bin.value.getObject();
		assertEquals("aValue", value.get("aKey"));
	}
	
	@Test
	public void testMapSetName() throws MappingError {
		Map<String, TopicConfig> topicConfigs = createTopicConfigs("testTopic", "topicNamespace", "topicSet");
		Map<String, Object> config = new HashMap<>();
		config.put(BaseMapperConfig.SET_FIELD_CONFIG, "bin1");
		JsonObjectMapper subject = new JsonObjectMapper();
		subject.configure(config);
		subject.setTopicConfigs(topicConfigs);
		SinkRecord record = createSinkRecord("testTopic", "testKey", "bin1", "aString", "bin2", 12345);

		KeyAndBins result = subject.convertRecord(record);

		Key askey = result.getKey();
		assertEquals("topicNamespace", askey.namespace);
		assertEquals("aString", askey.setName);
		assertEquals("testKey", askey.userKey.toString());
	}
	
	@Test
	public void testMapStringKey() throws MappingError {
		Map<String, TopicConfig> topicConfigs = createTopicConfigs("testTopic", "topicNamespace", "topicSet");
		Map<String, Object> config = new HashMap<>();
		config.put(BaseMapperConfig.KEY_FIELD_CONFIG, "bin1");
		config.put(BaseMapperConfig.KEY_TYPE_CONFIG, "string");
		JsonObjectMapper subject = new JsonObjectMapper();
		subject.configure(config);
		subject.setTopicConfigs(topicConfigs);
		SinkRecord record = createSinkRecord("testTopic", "testKey", "bin1", "aString", "bin2", 12345);

		KeyAndBins result = subject.convertRecord(record);

		Key askey = result.getKey();
		assertEquals("topicNamespace", askey.namespace);
		assertEquals("topicSet", askey.setName);
		assertEquals("aString", askey.userKey.toString());
	}

	@Test
	public void testMapIntegerKey() throws MappingError {
		Map<String, TopicConfig> topicConfigs = createTopicConfigs("testTopic", "topicNamespace", "topicSet");
		Map<String, Object> config = new HashMap<>();
		config.put(BaseMapperConfig.KEY_FIELD_CONFIG, "bin2");
		config.put(BaseMapperConfig.KEY_TYPE_CONFIG, "integer");
		JsonObjectMapper subject = new JsonObjectMapper();
		subject.configure(config);
		subject.setTopicConfigs(topicConfigs);
		SinkRecord record = createSinkRecord("testTopic", "testKey", "bin1", "aString", "bin2", 12345);

		KeyAndBins result = subject.convertRecord(record);

		Key askey = result.getKey();
		assertEquals("topicNamespace", askey.namespace);
		assertEquals("topicSet", askey.setName);
		assertEquals(12345, askey.userKey.toInteger());
	}

	@Test
	public void testMapLongKey() throws MappingError {
		Map<String, TopicConfig> topicConfigs = createTopicConfigs("testTopic", "topicNamespace", "topicSet");
		Map<String, Object> config = new HashMap<>();
		config.put(BaseMapperConfig.KEY_FIELD_CONFIG, "bin2");
		config.put(BaseMapperConfig.KEY_TYPE_CONFIG, "long");
		JsonObjectMapper subject = new JsonObjectMapper();
		subject.configure(config);
		subject.setTopicConfigs(topicConfigs);
		SinkRecord record = createSinkRecord("testTopic", "testKey", "bin1", "aString", "bin2", 12345678l);

		KeyAndBins result = subject.convertRecord(record);

		Key askey = result.getKey();
		assertEquals("topicNamespace", askey.namespace);
		assertEquals("topicSet", askey.setName);
		assertEquals(12345678l, askey.userKey.toLong());
	}

	@Test
	public void testMapBytesKey() throws MappingError {
		Map<String, TopicConfig> topicConfigs = createTopicConfigs("testTopic", "topicNamespace", "topicSet");
		Map<String, Object> config = new HashMap<>();
		config.put(BaseMapperConfig.KEY_FIELD_CONFIG, "bin2");
		config.put(BaseMapperConfig.KEY_TYPE_CONFIG, "bytes");
		JsonObjectMapper subject = new JsonObjectMapper();
		subject.configure(config);
		subject.setTopicConfigs(topicConfigs);
		SinkRecord record = createSinkRecord("testTopic", "testKey", "bin1", "aString", "bin2", new byte[] {0x01, 0x02, 0x03, 0x04});

		KeyAndBins result = subject.convertRecord(record);

		Key askey = result.getKey();
		assertEquals("topicNamespace", askey.namespace);
		assertEquals("topicSet", askey.setName);
		assertArrayEquals(new byte[] {0x01, 0x02, 0x03, 0x04}, (byte[])askey.userKey.getObject());
	}
	
	private Map<String, TopicConfig> createTopicConfigs(String topic, String namespace, String set) {
		Map<String, Object> config = new HashMap<>();
		config.put("namespace", namespace);
		config.put("set", set);
		return Collections.singletonMap(topic, new TopicConfig(config));
	}
	
	private SinkRecord createSinkRecord(String topic, Object key, Object ...keysAndValues) {
		int partition = 0;
		Schema keySchema = null;
		Schema valueSchema = null;
		Map<Object, Object> value = new HashMap<>();
		for (int i = 0; i < keysAndValues.length; i = i + 2) {
			value.put(keysAndValues[i], keysAndValues[i+1]);
		}
		long offset = 0;
		return new SinkRecord(topic, partition, keySchema, key, valueSchema, value, offset);
	}

}
