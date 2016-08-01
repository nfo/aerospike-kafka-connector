package com.aerospike.kafka.connect.mapper;

import java.util.Map;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.Key;
import com.aerospike.kafka.connect.sink.TopicConfig;

public abstract class AbstractRecordMapper implements RecordMapper, Configurable {

	private static final Logger log = LoggerFactory.getLogger(AbstractRecordMapper.class);
	
	private Map<String, ?> config;
	private Map<String, TopicConfig> topicConfigs;

	public abstract KeyAndBins convertRecord(SinkRecord record) throws MappingError;

	public void configure(Map<String, ?> config) {
		log.debug("Configuring {}: {}", this.getClass(), config);
		this.config = config;
	}

	public Map<String, ?> getConfig() {
		return config;
	}

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