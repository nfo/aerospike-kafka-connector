package com.aerospike.kafka.connect.sink;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AerospikeSinkConnector extends SinkConnector {

	static final String HOSTNAME_CONFIG = "hostname";
	static final String PORT_CONFIG = "port";
	static final String NAMESPACE_CONFIG = "namespace";
	static final String SET_CONFIG = "set";
	static final String POLICY_RECORD_EXISTS_ACTION = "policy.record_exists_action";

	private static final Logger log = LoggerFactory.getLogger(AerospikeSinkConnector.class);
	private static final ConfigDef CONFIG_DEF = new ConfigDef()
			.define(HOSTNAME_CONFIG, Type.STRING, Importance.HIGH, "Aerospike Hostname")
			.define(PORT_CONFIG, Type.INT, 3000, Range.between(1, 65535), Importance.LOW, "Aerospike Port")
			.define(NAMESPACE_CONFIG, Type.STRING, Importance.HIGH, "Aerospike Namespace")
			.define(SET_CONFIG, Type.STRING, Importance.HIGH, "Aerospike Set")
			.define(POLICY_RECORD_EXISTS_ACTION, Type.STRING, "update",
					ValidString.in("create_only", "update", "update_only", "replace", "replace_only"), Importance.LOW,
					"Write Policy: How to handle writes when the record already exists");

	private Map<String, String> props;

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public void start(Map<String, String> props) {
		log.trace("Starting {} connector with config: {}", this.getClass().getName(), props);
		this.props = props;
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
		return Collections.singletonList(props);
	}

	@Override
	public String version() {
		return AppInfoParser.getVersion();
	}

}
