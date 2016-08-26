/**
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

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.Host;

public class HostsParser {
    public static final int DEFAULT_PORT = 3000;

    /**
     * Parses a comma separated list of hosts; each host consists of a host name
     * or IP with an optional port number separated by colon.
     * 
     * @param hosts Comma separated list of hosts
     * @return Array of Host objects
     */
    public static Host[] parseHostsString(String hosts) {
        List<Host> hostList = new ArrayList<>();
        for (String hostWithPort : hosts.split(",")) {
            String[] hostAndPort = hostWithPort.split(":", 2);
            String host = hostAndPort[0];
            int port = DEFAULT_PORT;
            if (hostAndPort.length == 2) {
                port = Integer.valueOf(hostAndPort[1], 10);
            }
            hostList.add(new Host(host, port));
        }
        return hostList.toArray(new Host[0]);
    }

}
