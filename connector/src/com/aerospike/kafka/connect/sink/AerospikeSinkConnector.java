package com.aerospike.kafka.connect.sink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AerospikeSinkConnector extends SinkConnector {

	private static final Logger log = LoggerFactory.getLogger(AerospikeSinkConnector.class);

	private Map<String, String> configProperties;

	@Override
	public ConfigDef config() {
		return AerospikeSinkConnectorConfig.config;
	}

	@Override
	public void start(Map<String, String> props) {
		log.trace("Starting {} connector with config: {}", this.getClass().getName(), props);
		try {
			configProperties = props;
			new AerospikeSinkConnectorConfig(props);
		} catch (ConfigException e) {
			throw new ConnectException("Could not start AerospikeSinkConnector due to configuration error", e);
		}
	}

	@Override
	public void stop() {
		// Nothing to do since AerospikeSinkConnector has no background monitoring
		log.trace("Stopping {} connector", this.getClass().getName());
	}

	@Override
	public Class<? extends Task> taskClass() {
		return AerospikeSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		List<Map<String, String>> taskConfigs = new ArrayList<>();
		Map<String, String> taskProps = new HashMap<>();
		taskProps.putAll(configProperties);
		for (int i = 0; i < maxTasks; i++) {
			taskConfigs.add(taskProps);
		}
		return taskConfigs;
	}

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

}
