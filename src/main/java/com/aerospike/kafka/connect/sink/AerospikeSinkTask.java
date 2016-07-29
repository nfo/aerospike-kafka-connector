package com.aerospike.kafka.connect.sink;

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.kafka.connect.errors.ConversionError;

public class AerospikeSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(AerospikeSinkTask.class);
	
	private RecordConverter converter;
	private AerospikeClient client;
	private WritePolicy writePolicy;
	private String namespace;
	private String set;

	public AerospikeSinkTask() {
		converter = new RecordConverter();
	}

	public String version() {
		return new AerospikeSinkConnector().version();
	}

	@Override
	public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
		// TODO Auto-generated method stub

	}

	@Override
	public void put(Collection<SinkRecord> sinkRecords) {
		for (SinkRecord record : sinkRecords) {
			try {
				KeyAndBins keyAndBins = converter.convertRecord(record, namespace, set);
				Key key = keyAndBins.getKey();
				Bin[] bins = keyAndBins.getBins();
				log.trace("Writing record for key {}: {}", key, bins);
				client.put(writePolicy, key, bins);
			} catch (ConversionError e) {
				log.error("Error converting record", e);
			} catch (AerospikeException e) {
				log.error("Error writing to record", e);
			}
		}
	}

	@Override
	public void start(Map<String, String> props) {
		log.trace("Starting {} task with config: {}", this.getClass().getName(), props);
		AerospikeSinkConnectorConfig config = new AerospikeSinkConnectorConfig(props);
		try {
			String hostname = config.getHostname();
			int port = config.getPort();
			ClientPolicy policy = new ClientPolicy();
			client = new AerospikeClient(policy, hostname, port);
		} catch (AerospikeException e) {
			throw new ConnectException("Could not connect to Aerospike cluster", e);
		}

		writePolicy = createWritePolicy(config);
		namespace = config.getNamespace();
		set = config.getSet();
	}

	@Override
	public void stop() {
		log.trace("Stopping {} task", this.getClass().getName());
	}

	private WritePolicy createWritePolicy(AerospikeSinkConnectorConfig config) {
		WritePolicy policy = new WritePolicy();
		RecordExistsAction action = config.getPolicyRecordExistsAction();
		if (action != null) {
			policy.recordExistsAction = action;
		}
		log.trace("Write Policy: recordExistsAction={}", policy.recordExistsAction);
		return policy;
	}

}
