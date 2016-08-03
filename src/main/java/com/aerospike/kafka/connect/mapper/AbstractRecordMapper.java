package com.aerospike.kafka.connect.mapper;

import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.sink.SinkRecord;

import com.aerospike.client.Key;
import com.aerospike.kafka.connect.sink.TopicConfig;

public abstract class AbstractRecordMapper implements RecordMapper, Configurable {

	private BaseMapperConfig config;
	private Map<String, TopicConfig> topicConfigs;

	public abstract KeyAndBins convertRecord(SinkRecord record) throws MappingError;

	public void configure(Map<String, ?> props) {
		this.config = new BaseMapperConfig(props);
	}
	
	protected BaseMapperConfig getConfig() {
		return config;
	}

	public void setTopicConfigs(Map<String, TopicConfig> topicConfigs) {
		this.topicConfigs = topicConfigs;
	}
	
	protected TopicConfig getTopicConfig(SinkRecord record) {
		String topic = record.topic();
		return topicConfigs.get(topic);
	}

	protected Key createKey(String namespace, String set, Object userKey, String keyType) throws MappingError {
		switch(keyType) {
		case "string":
			return new Key(namespace, set, (String)userKey);
		case "integer":
			return new Key(namespace, set, (int)userKey);
		case "long":
			return new Key(namespace, set, (long)userKey);
		case "bytes":
			return new Key(namespace, set, (byte[])userKey);
		default:
			// This should never happen if the configuration is validated!
			throw new MappingError("Unsupported key type: " + keyType);
		}
	}

}