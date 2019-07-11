/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.ClientPartitionListenerService;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientGetPartitionsCodec;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientPartitionService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.spi.impl.listener.AbstractClientListenerService;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.memberselector.MemberSelectors;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.cp.internal.util.Tuple2;
import com.hazelcast.internal.cluster.impl.MemberSelectingCollection;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionTableView;
import com.hazelcast.internal.partition.client.ClientAddPartitionTableListenerCodec;
import com.hazelcast.internal.partition.client.ClientGetPartitionTableCodec;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.partition.Partition;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.HashUtil;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * The {@link ClientPartitionService} implementation.
 */
public final class ClientPartitionServiceImpl implements ClientPartitionService {

    private static final long PERIOD = 10;
    private static final long INITIAL_DELAY = 10;
    private static final long BLOCKING_GET_ONCE_SLEEP_MILLIS = 100;
    private final ExecutionCallback<ClientMessage> refreshTaskCallback = new RefreshTaskCallback();
    private final ClientExecutionServiceImpl clientExecutionService;
    private final HazelcastClientInstanceImpl client;
    private final ILogger logger;
    private final AtomicReference<PartitionTable> partitionTable =
            new AtomicReference<>(new PartitionTable(null, -1, new Address[0][InternalPartition.MAX_REPLICA_COUNT]));
    private volatile int partitionCount;
    private volatile long lastCorrelationId = -1;

    public ClientPartitionServiceImpl(HazelcastClientInstanceImpl client) {
        this.client = client;
        this.logger = client.getLoggingService().getLogger(ClientPartitionService.class);
        clientExecutionService = (ClientExecutionServiceImpl) client.getClientExecutionService();
    }

    public void start() {
        //scheduling left in place to support server versions before 3.9.
        clientExecutionService.scheduleWithRepetition(new RefreshTask(), INITIAL_DELAY, PERIOD, TimeUnit.SECONDS);
    }

    private static class PartitionTable {
        final Connection connection;
        final int partitionSateVersion;
        final Address[][] partitions;

        PartitionTable(Connection connection, int partitionSateVersion, Address[][] partitions) {
            this.connection = connection;
            this.partitionSateVersion = partitionSateVersion;
            this.partitions = partitions;
        }
    }

    public void listenPartitionTable(Connection ownerConnection) throws Exception {
        //when we connect to cluster back we need to reset partition state version
        //we are keeping the partition map as is because, user may want its operations run on connected members even if
        //owner connection is gone, and partition table is missing.
        // See @{link com.hazelcast.client.spi.properties.ClientProperty#ALLOW_INVOCATIONS_WHEN_DISCONNECTED}
        Address[][] partitions = getPartitions();
        partitionTable.set(new PartitionTable(ownerConnection, -1, partitions));

        ClientMessage clientMessage = ClientAddPartitionTableListenerCodec.encodeRequest();
        ClientInvocation invocation = new ClientInvocation(client, clientMessage, null, ownerConnection);
        invocation.setEventHandler(new PartitionEventHandler(ownerConnection));
        invocation.invokeUrgent().get();
        lastCorrelationId = clientMessage.getCorrelationId();
    }

    public void cleanupOnDisconnect() {
        ((AbstractClientListenerService) client.getListenerService()).removeEventHandler(lastCorrelationId);
    }

    void refreshPartitions() {
        try {
            // use internal execution service for all partition refresh process (do not use the user executor thread)
            clientExecutionService.execute(new RefreshTask());
        } catch (RejectedExecutionException ignored) {
            ignore(ignored);
        }
    }

    private void waitForPartitionCountSetOnce() {
        while (partitionCount == 0 && client.getConnectionManager().isAlive()) {
            ClientClusterService clusterService = client.getClientClusterService();
            Collection<Member> memberList = clusterService.getMemberList();
            Connection currentOwnerConnection = this.partitionTable.get().connection;
            //if member list is empty or owner is null, we will sleep and retry
            if (memberList.isEmpty() || currentOwnerConnection == null) {
                sleepBeforeNextTry();
                continue;
            }
            if (isClusterFormedByOnlyLiteMembers(memberList)) {
                throw new NoDataMemberInClusterException(
                        "Partitions can't be assigned since all nodes in the cluster are lite members");
            }

            ClientMessage requestMessage = ClientGetPartitionsCodec.encodeRequest();
            // invocation should go to owner connection because the listener is added to owner connection
            // and partition state version should be fetched from one member to keep it consistent
            ClientInvocationFuture future =
                    new ClientInvocation(client, requestMessage, null, currentOwnerConnection).invokeUrgent();
            try {
                ClientMessage responseMessage = future.get();
                ClientGetPartitionTableCodec.ResponseParameters response =
                        ClientGetPartitionTableCodec.decodeResponse(responseMessage);
                Connection connection = responseMessage.getConnection();
                processPartitionResponse(connection, response.partitions,
                        response.partitionStateVersion, response.partitionStateVersionExist);
            } catch (Exception e) {
                if (client.getLifecycleService().isRunning()) {
                    logger.warning("Error while fetching cluster partition table!", e);
                }
            }
        }
    }

    private void sleepBeforeNextTry() {
        try {
            Thread.sleep(BLOCKING_GET_ONCE_SLEEP_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw ExceptionUtil.rethrow(e);
        }
    }

    private boolean isClusterFormedByOnlyLiteMembers(Collection<Member> memberList) {
        return MemberSelectingCollection.count(memberList, MemberSelectors.DATA_MEMBER_SELECTOR) == 0;
    }

    /**
     * The partitions can be empty on the response, client will not apply the empty partition table,
     * see {@link ClientPartitionListenerService#getPartitions(PartitionTableView)}
     */
    private void processPartitionResponse(Connection connection, Collection<Map.Entry<Address, List<Tuple2<Integer, Integer>>>> partitions,
                                          int partitionStateVersion,
                                          boolean partitionStateVersionExist) {

        while (true) {
            PartitionTable current = this.partitionTable.get();
            if (!shouldBeApplied(connection, partitions, partitionStateVersion, partitionStateVersionExist, current)) {
                return;
            }
            Address[][] newPartitions = convertToPartitionToAddressMap(partitions);
            PartitionTable newMetaData = new PartitionTable(connection, partitionStateVersion, newPartitions);
            if (this.partitionTable.compareAndSet(current, newMetaData)) {
                // partition count is set once at the start. Even if we reset the partition table when switching cluster
                //we want to remember the partition count. That is why it is a different field.
                if (partitionCount == 0) {
                    partitionCount = newPartitions.length;
                }
                if (logger.isFinestEnabled()) {
                    logger.finest("Processed partition response. partitionStateVersion : "
                            + (partitionStateVersionExist ? partitionStateVersion : "NotAvailable")
                            + ", partitionCount :" + newPartitions.length + ", connection : " + connection);
                }
                return;
            }

        }
    }

    private boolean shouldBeApplied(Connection connection, Collection<Entry<Address, List<Tuple2<Integer, Integer>>>> partitions,
                                    int partitionStateVersion, boolean partitionStateVersionExist, PartitionTable current) {
        if (partitions.isEmpty()) {
            if (logger.isFinestEnabled()) {
                logFailure(connection, partitionStateVersion, partitionStateVersionExist, current,
                        "response is empty");
            }
            return false;
        }
        if (!connection.equals(current.connection)) {
            if (logger.isFinestEnabled()) {
                logFailure(connection, partitionStateVersion, partitionStateVersionExist, current,
                        "response is from old connection");
            }
            return false;
        }
        if (partitionStateVersionExist && partitionStateVersion <= current.partitionSateVersion) {
            if (logger.isFinestEnabled()) {
                logFailure(connection, partitionStateVersion, partitionStateVersionExist, current,
                        "response state version is old");
            }
            return false;
        }
        return true;
    }

    private void logFailure(Connection connection, int partitionStateVersion,
                            boolean partitionStateVersionExist, PartitionTable current, String cause) {

        logger.finest(" We will not apply the response, since " + cause + " . Response is from " + connection
                + ". Current connection " + current.connection
                + " response state version:"
                + (partitionStateVersionExist ? partitionStateVersion : "NotAvailable"
                + ". Current state version: " + current.partitionSateVersion));
    }

    private Address[][] convertToPartitionToAddressMap(Collection<Entry<Address, List<Tuple2<Integer, Integer>>>> partitions) {
        int partitionCount = partitions.stream().flatMap(e -> e.getValue().stream()).mapToInt(t -> t.element1).max().orElse(0) + 1;
        Address[][] newPartitions = new Address[partitionCount][InternalPartition.MAX_REPLICA_COUNT];
        for (Entry<Address, List<Tuple2<Integer, Integer>>> entry : partitions) {
            Address address = entry.getKey();
            for (Tuple2<Integer, Integer> partition : entry.getValue()) {
                newPartitions[partition.element1][partition.element2] = address;
            }
        }
        return newPartitions;
    }

    public void reset() {
        partitionTable.set(new PartitionTable(null, -1, new Address[partitionCount][InternalPartition.MAX_REPLICA_COUNT]));
    }

    @Override
    public Address getPartitionOwner(int partitionId) {
        return getReplicaOwner(partitionId, 0);
    }

    @Override
    public Address getReplicaOwner(int partitionId, int replicaIndex) {
        Address[][] partitions = getPartitions();
        if (partitions.length <= partitionId) {
            return null;
        }
        return partitions[partitionId][replicaIndex];
    }

    private Address[][] getPartitions() {
        return partitionTable.get().partitions;
    }

    @Override
    public int getPartitionId(Data key) {
        final int pc = getPartitionCount();
        if (pc <= 0) {
            return 0;
        }
        int hash = key.getPartitionHash();
        return HashUtil.hashToIndex(hash, pc);
    }

    @Override
    public int getPartitionId(Object key) {
        final Data data = client.getSerializationService().toData(key);
        return getPartitionId(data);
    }

    @Override
    public int getPartitionCount() {
        if (partitionCount == 0) {
            waitForPartitionCountSetOnce();
        }
        return partitionCount;
    }

    @Override
    public Partition getPartition(int partitionId) {
        return new PartitionImpl(partitionId);
    }

    private final class PartitionImpl implements Partition {

        private final int partitionId;

        private PartitionImpl(int partitionId) {
            this.partitionId = partitionId;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public Member getOwner() {
            Address owner = getPartitionOwner(partitionId);
            if (owner != null) {
                return client.getClientClusterService().getMember(owner);
            }
            return null;
        }

        @Override
        public String toString() {
            return "PartitionImpl{partitionId=" + partitionId + '}';
        }
    }

    private final class PartitionEventHandler extends ClientAddPartitionTableListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final Connection clientConnection;

        private PartitionEventHandler(Connection clientConnection) {
            this.clientConnection = clientConnection;
        }

        @Override
        public void handlePartitionsEventV15(Collection<Entry<Address, List<Tuple2<Integer, Integer>>>> response,
                int stateVersion) {
            processPartitionResponse(clientConnection, response, stateVersion, true);
        }

        @Override
        public void beforeListenerRegister() {

        }

        @Override
        public void onListenerRegister() {

        }
    }

    private final class RefreshTask implements Runnable {

        private RefreshTask() {
        }

        @Override
        public void run() {
            try {
                ClientConnectionManager connectionManager = client.getConnectionManager();
                Connection connection = connectionManager.getOwnerConnection();
                if (connection == null) {
                    return;
                }
                ClientMessage requestMessage = ClientGetPartitionsCodec.encodeRequest();
                ClientInvocationFuture future = new ClientInvocation(client, requestMessage, null).invokeUrgent();
                future.andThen(refreshTaskCallback);
            } catch (Exception e) {
                if (client.getLifecycleService().isRunning()) {
                    logger.warning("Error while fetching cluster partition table!", e);
                }
            }
        }
    }

    private class RefreshTaskCallback implements ExecutionCallback<ClientMessage> {

        @Override
        public void onResponse(ClientMessage responseMessage) {
            if (responseMessage == null) {
                return;
            }
            Connection connection = responseMessage.getConnection();
            ClientGetPartitionTableCodec.ResponseParameters response = ClientGetPartitionTableCodec.decodeResponse(responseMessage);
            processPartitionResponse(connection, response.partitions,
                    response.partitionStateVersion, response.partitionStateVersionExist);
        }

        @Override
        public void onFailure(Throwable t) {
            if (client.getLifecycleService().isRunning()) {
                logger.warning("Error while fetching cluster partition table!", t);
            }
        }
    }
}
