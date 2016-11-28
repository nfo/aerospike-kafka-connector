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

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.policy.ClientPolicy;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.AerospikeException.CommandRejected;
import com.aerospike.client.AerospikeException.Connection;
import com.aerospike.client.AerospikeException.Timeout;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.kafka.connect.data.AerospikeRecord;

/**
 * The SyncWriter handles connections to the Aerospike cluster, sending data
 * and flush. The write sends individual request to write each record using the
 * async client. The flush method waits until all in-flight request have been
 * completed.
 */
public class SyncWriter {

    private static final Logger log = LoggerFactory.getLogger(SyncWriter.class);

    private final AerospikeClient client;
    private final WritePolicy writePolicy;

    public SyncWriter(ConnectorConfig config) {
        try {
            Host[] hosts = config.getHosts();
            ClientPolicy policy = createClientPolicy(config);
            client = new AerospikeClient(policy, hosts);
        } catch (AerospikeException e) {
            throw new ConnectException("Error connecting to Aerospike cluster", e);
        }
        writePolicy = createWritePolicy(config);
        log.info("NF: {} created non-async client: {}", this.getClass().getName(), client);
    }

    public void write(AerospikeRecord record) {
        Key key = record.key();
        Bin[] bins = record.bins();
        try {
            client.put(writePolicy, key, bins);
        } catch (AerospikeException e) {
            String message = "Error writing record: " + e.getMessage();
            boolean retriable = (e instanceof CommandRejected || e instanceof Timeout || e instanceof Connection);
            if (retriable) {
                throw new RetriableException(message, e);
            } else {
                throw new ConnectException(message, e);
            }
        }
    }

    public void flush() {
        log.info("NF: {} flushing (does nothing on non-async)", this.getClass().getName());
    }
    
    public void close() {
        log.trace("NF: {} calling `client.close()`", this.getClass().getName());
        client.close();
    }

    private ClientPolicy createClientPolicy(ConnectorConfig config) {
        ClientPolicy policy = new ClientPolicy();
        return policy;
    }

    private WritePolicy createWritePolicy(ConnectorConfig config) {
        WritePolicy policy = new WritePolicy();
        RecordExistsAction action = config.getPolicyRecordExistsAction();
        if (action != null) {
            policy.recordExistsAction = action;
        }
        log.trace("Write Policy: recordExistsAction={}", policy.recordExistsAction);
        return policy;
    }
    
}