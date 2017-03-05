/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.memcached;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.spotify.folsom.ConnectFuture;
import com.spotify.folsom.MemcacheClient;
import com.spotify.folsom.MemcacheClientBuilder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.annotations.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Experimental
public class MemcachedIO {
    private static final Logger LOG = LoggerFactory.getLogger(MemcachedIO.class);

    /** Disallow construction of utility class. */
    private MemcachedIO() {
    }

//    public static Read read() {
//        return new Read(null, "", new SerializableScan(new Scan()));
//    }

    public static void main(String[] args) {
        String host = "localhost";
        List<String> addresses = Collections.singletonList(host);
        List<HostAndPort> hostandPorts = new ArrayList<>();
        for (String address : addresses) {
            hostandPorts.add(HostAndPort.fromString(address));
        }
        final MemcacheClient<String> client = MemcacheClientBuilder.newStringClient()
                .withAddresses(hostandPorts)
//                .connectBinary()
                .connectAscii();
// make we wait until the client has connected to the server
        try {
            ListenableFuture<Void> listenableFuture = ConnectFuture.connectFuture(client);
            listenableFuture.get();
            client.set("key", "value", 10000).get();
            client.get("key").get();
            client.shutdown();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }
}
