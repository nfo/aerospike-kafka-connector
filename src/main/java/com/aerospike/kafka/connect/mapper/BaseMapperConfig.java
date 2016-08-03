package com.aerospike.kafka.connect.mapper;

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.config.ConfigDef.Validator;

public class BaseMapperConfig extends AbstractConfig {

	public static final String CLASS_CONFIG = "class";
	private static final String CLASS_DOC = "Name of the RecordMapper class";
	private static final Class<? extends RecordMapper> CLASS_DEFAULT = JsonObjectMapper.class;
	
	public static final String KEY_FIELD_CONFIG = "key_field";
	private static final String KEY_FIELD_DOC = "Name of the record field that contains the record key";

	public static final String KEY_TYPE_CONFIG = "key_type";
	private static final String KEY_TYPE_DOC = "Type of the key";
	private static final String KEY_TYPE_DEFAULT = "string";
	private static final Validator KEY_TYPE_VALIDATOR = ValidString.in("string", "integer", "long", "bytes");

	public static final String SET_FIELD_CONFIG = "set_field";
	private static final String SET_FIELD_DOC = "Name of the record field that contains the set name";
	
	public static ConfigDef baseConfigDef() {
		return new ConfigDef()
				.define(CLASS_CONFIG, Type.CLASS, CLASS_DEFAULT, Importance.HIGH, CLASS_DOC)
				.define(KEY_FIELD_CONFIG, Type.STRING, null, Importance.MEDIUM, KEY_FIELD_DOC)
				.define(KEY_TYPE_CONFIG, Type.STRING, KEY_TYPE_DEFAULT, KEY_TYPE_VALIDATOR, Importance.MEDIUM, KEY_TYPE_DOC)
				.define(SET_FIELD_CONFIG, Type.STRING, null, Importance.MEDIUM, SET_FIELD_DOC);
	}
	
	public static ConfigDef config = baseConfigDef();
	
	public BaseMapperConfig(Map<String, ?> props) {
		super(config, props);
	}
	
	public String getKeyField() {
		return getString(KEY_FIELD_CONFIG);
	}

	public String getKeyType() {
		return getString(KEY_TYPE_CONFIG);
	}

	public String getSetField() {
		return getString(SET_FIELD_CONFIG);
	}

	public RecordMapper getMapperInstance() {
		return getConfiguredInstance(CLASS_CONFIG, RecordMapper.class);
	}
}