package com.aerospike.kafka.connect.mapper;

import java.util.Map;

import org.apache.kafka.connect.sink.SinkRecord;

import com.aerospike.client.Key;
import com.aerospike.kafka.connect.sink.TopicConfig;

public abstract class AbstractRecordMapper implements RecordMapper {

	private Map<String, TopicConfig> topicConfigs;

	public abstract KeyAndBins convertRecord(SinkRecord record) throws MappingError;

	public void setTopicConfigs(Map<String, TopicConfig> topicConfigs) {
		this.topicConfigs = topicConfigs;
	}
	
	protected TopicConfig getTopicConfig(SinkRecord record) {
		String topic = record.topic();
		return topicConfigs.get(topic);
	}

	protected Key keyFromRecord(SinkRecord record) throws MappingError {
		Object key = record.key();
		TopicConfig config = getTopicConfig(record);
		if (key instanceof String) {
			return new Key(config.getNamespace(), config.getSet(), (String)key);
		} else {
			throw new MappingError("Unsupported record key: " + key);
		}
	}

}