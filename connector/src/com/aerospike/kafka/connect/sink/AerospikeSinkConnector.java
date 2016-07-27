package com.aerospike.kafka.connect.sink;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class AerospikeSinkConnector extends SinkConnector {

	static final String HOSTNAME_CONFIG = "hostname";
	static final String PORT_CONFIG = "port";
	static final String NAMESPACE_CONFIG = "namespace";
	static final String SET_CONFIG = "set";
	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(HOSTNAME_CONFIG, Type.STRING, Importance.HIGH, "Aerospike Hostname")
			.define(PORT_CONFIG, Type.INT, 3000, Range.between(1, 65535), Importance.LOW, "Aerospike Port")
			.define(NAMESPACE_CONFIG, Type.STRING, Importance.HIGH, "Aerospike Namespace")
			.define(SET_CONFIG, Type.STRING, Importance.HIGH, "Aerospike Set");

	private String hostname;
	private String port;
	private String namespace;
	private String set;
	
	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public void start(Map<String, String> props) {
		hostname = props.get(HOSTNAME_CONFIG);
		port = props.getOrDefault(PORT_CONFIG, "3000");
		namespace = props.get(NAMESPACE_CONFIG);
		set = props.get(SET_CONFIG);
	}

	@Override
	public void stop() {
		// Nothing to do since AerospikeSinkConnector has no background monitoring
	}

	@Override
	public Class<? extends Task> taskClass() {
		return AerospikeSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		Map<String, String> config = new HashMap<>();
		config.put(HOSTNAME_CONFIG, hostname);
		config.put(PORT_CONFIG, port);
		config.put(NAMESPACE_CONFIG, namespace);
		config.put(SET_CONFIG, set);
		return Collections.singletonList(config);
	}

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

}
