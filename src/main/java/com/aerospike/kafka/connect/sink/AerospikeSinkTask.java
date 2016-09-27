/*
 * Copyright 2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.aerospike.kafka.connect.sink;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.kafka.connect.data.AerospikeRecord;
import com.aerospike.kafka.connect.data.RecordConverter;
import com.aerospike.kafka.connect.data.RecordMapperFactory;

public class AerospikeSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(AerospikeSinkTask.class);

    private RecordMapperFactory mappers;
    private AsyncWriter writer;

    private long lastFlushTimeMillis = 0;
    private Map<TopicPartition, OffsetAndMetadata> lastOffsets;

    public AerospikeSinkTask() {
    }

    public String version() {
        return new AerospikeSinkConnector().version();
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        if (log.isInfoEnabled()) {
            report(offsets);
        }
        writer.flush();
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord sinkRecord : sinkRecords) {
            try {
                RecordConverter mapper = mappers.getMapper(sinkRecord);
                AerospikeRecord record = mapper.convertRecord(sinkRecord);
                Key key = record.key();
                Bin[] bins = record.bins();
                log.trace("Writing record for key {}: {}", key, bins);
                writer.write(record);
            } catch (AerospikeException e) {
                log.error("Error writing to record", e);
            }
        }
    }

    @Override
    public void start(Map<String, String> props) {
        log.trace("Starting {} task with config: {}", this.getClass().getName(), props);
        ConnectorConfig config = new ConnectorConfig(props);
        mappers = new RecordMapperFactory(config.getTopicConfigs());
        writer = new AsyncWriter(config);
    }

    @Override
    public void stop() {
        log.trace("Stopping {} task", this.getClass().getName());
        if (writer != null) {
            writer.close();
        }
    }

    private void report(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        long now = System.currentTimeMillis();
        if (lastFlushTimeMillis > 0) {
            long elapsedMs = now - lastFlushTimeMillis;
            for (Entry<TopicPartition, OffsetAndMetadata> entry : currentOffsets.entrySet()) {
                TopicPartition partition = entry.getKey();
                OffsetAndMetadata lastOffset = lastOffsets.get(partition);
                if (lastOffset == null) {
                    continue;
                }
                OffsetAndMetadata currentOffset = entry.getValue();
                long records = currentOffset.offset() - lastOffset.offset();
                log.info("Wrote {} records in {} ms for topic {}, partition {} - throughput: {} TPS", records,
                        elapsedMs, partition.topic(), partition.partition(), Math.round(1000.0 * records / elapsedMs));
            }
        }
        lastFlushTimeMillis = now;
        lastOffsets = currentOffsets;
    }
}
