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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.async.AsyncClient;
import com.aerospike.client.async.AsyncClientPolicy;
import com.aerospike.client.async.MaxCommandAction;
import com.aerospike.client.listener.WriteListener;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.kafka.connect.data.AerospikeRecord;

public class AsyncWriter implements WriteListener {

    // TODO: make limit & sleep interval configurable
    private static final int MAX_COMMANDS = 300;
    private static final long SLEEP_INTERVAL = 10;

    private static final Logger log = LoggerFactory.getLogger(AsyncWriter.class);

    private AsyncClient client;
    private WritePolicy writePolicy;
    private AtomicCounter inFlight = new AtomicCounter(SLEEP_INTERVAL);

    public AsyncWriter(ConnectorConfig config) {
        try {
            String hostname = config.getHostname();
            int port = config.getPort();
            AsyncClientPolicy policy = new AsyncClientPolicy();
            policy.asyncMaxCommandAction = MaxCommandAction.BLOCK;
            policy.asyncMaxCommands = MAX_COMMANDS;
            client = new AsyncClient(policy, hostname, port);
        } catch (AerospikeException e) {
            throw new ConnectException("Error connecting to Aerospike cluster", e);
        }
        writePolicy = createWritePolicy(config);
    }

    public void write(AerospikeRecord record) {
        Key key = record.key();
        Bin[] bins = record.bins();
        inFlight.incr();
        client.put(writePolicy, this, key, bins);
    }

    public void flush() {
        inFlight.waitUntilZero();
    }

    @Override
    public void onFailure(AerospikeException e) {
        log.error("Error writing record", e);
        inFlight.decr();
    }

    @Override
    public void onSuccess(Key key) {
        log.trace("Successfully put key {}", key);
        inFlight.decr();
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

    class AtomicCounter {
        private final long sleepMs;
        private AtomicInteger counter;

        public AtomicCounter(long sleepMs) {
            this.sleepMs = sleepMs;
            counter = new AtomicInteger(0);
        }

        public void incr() {
            counter.incrementAndGet();
        }

        public void decr() {
            counter.decrementAndGet();
        }

        public void waitUntilZero() {
            try {
                int count;
                while ((count = counter.get()) > 0) {
                    log.trace("Waiting " + sleepMs + "ms for counter to reach zero - current: " + count);
                    Thread.sleep(sleepMs);
                }
            } catch (InterruptedException e) {
                throw new ConnectException(e);
            }
        }
    }
}