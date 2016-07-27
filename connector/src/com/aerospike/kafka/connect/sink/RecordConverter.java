package com.aerospike.kafka.connect.sink;

import java.util.LinkedList;
import java.util.List;

import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;

public class RecordConverter {

	public Key convertKey(SinkRecord record, String namespace, String set) {
		Object key = record.key();
		Type type = record.keySchema().type();
		switch(type) {
		case STRING:
			return new Key(namespace, set, (String)key);
		case INT8:
		case INT16:
		case INT32:
			return new Key(namespace, set, (int)key);
		case INT64:
			return new Key(namespace, set, (long)key);
		case BYTES:
			return new Key(namespace, set, (byte[])key);
		default:
			throw new ConnectException(String.format("Unsupported key type: {}", type));
		}
	}

	public Bin[] convertValue(SinkRecord record) {
		List<Bin> bins = new LinkedList<>();
		Type type = record.valueSchema().type();
		switch(type) {
		case STRING:
			bins.add(new Bin("value", record.value().toString()));
			break;
		default:
			throw new ConnectException(String.format("Unsupported record type: {}", type));
		}
		Bin[] array = new Bin[bins.size()];
		return bins.toArray(array);
	}
}
