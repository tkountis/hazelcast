/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.nio;

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.NetworkStats;
import com.hazelcast.internal.nio.tcp.DefaultConnection;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.emptyMap;

public class DefaultAggregateEndpoint
        implements AggregateEndpoint {

    private final ConcurrentMap<EndpointQualifier, Endpoint<DefaultConnection>> endpointManagers;

    public DefaultAggregateEndpoint(ConcurrentMap<EndpointQualifier, Endpoint<DefaultConnection>> endpointManagers) {
        this.endpointManagers = endpointManagers;
    }

    @Override
    public Set<DefaultConnection> getActiveConnections() {
        Set<DefaultConnection> connections = null;
        for (Endpoint<DefaultConnection> endpointManager : endpointManagers.values()) {
            Collection<DefaultConnection> endpointConnections = endpointManager.getActiveConnections();
            if (endpointConnections != null && !endpointConnections.isEmpty()) {
                if (connections == null) {
                    connections = new HashSet<>();
                }

                connections.addAll(endpointConnections);
            }
        }

        return connections == null ? Collections.emptySet() : connections;
    }

    @Override
    public Set<DefaultConnection> getConnections() {
        Set<DefaultConnection> connections = null;

        for (Endpoint<DefaultConnection> endpointManager : endpointManagers.values()) {
            Collection<DefaultConnection> endpointConnections = endpointManager.getConnections();
            if (endpointConnections != null && !endpointConnections.isEmpty()) {
                if (connections == null) {
                    connections = new HashSet<>();
                }

                connections.addAll(endpointConnections);
            }
        }

        return connections == null ? Collections.emptySet() : connections;
    }

    public Endpoint<DefaultConnection> getEndpointManager(EndpointQualifier qualifier) {
        return endpointManagers.get(qualifier);
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        for (Endpoint manager : endpointManagers.values()) {
            manager.addConnectionListener(listener);
        }
    }

    @Override
    public Map<EndpointQualifier, NetworkStats> getNetworkStats() {
        Map<EndpointQualifier, NetworkStats> stats = null;
        for (Map.Entry<EndpointQualifier, Endpoint<DefaultConnection>> entry : endpointManagers.entrySet()) {
            if (stats == null) {
                stats = new HashMap<>();
            }
            stats.put(entry.getKey(), entry.getValue().getNetworkStats());
        }
        return stats == null ? emptyMap() : stats;
    }
}
