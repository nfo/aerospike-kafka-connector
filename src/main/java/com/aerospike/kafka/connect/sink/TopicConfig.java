package com.aerospike.kafka.connect.sink;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

public class TopicConfig extends AbstractConfig {

	private static final String NAMESPACE_CONFIG = "namespace";
	private static final String NAMESPACE_DOC = "Namespace to use for the topic";

	private static final String SET_CONFIG = "set";
	private static final String SET_DOC = "Set to use for the topic";

	public static ConfigDef baseConfigDef() {
		return new ConfigDef()
				.define(NAMESPACE_CONFIG, Type.STRING, Importance.LOW, NAMESPACE_DOC)
				.define(SET_CONFIG, Type.STRING, Importance.LOW, SET_DOC);
	}

	public static ConfigDef config = baseConfigDef();
	
	public TopicConfig(Map<String, String> props) {
		super(config, props);
	}
	
	public String getNamespace() {
		return getString(NAMESPACE_CONFIG);
	}
	
	public String getSet() {
		return getString(SET_CONFIG);
	}
}