/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorCancelCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorDisposeCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetDelayCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetResultCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorGetStatsCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsCancelledCodec;
import com.hazelcast.client.impl.protocol.codec.ScheduledExecutorIsDoneCodec;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.client.util.ClientDelegatingFuture;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.partition.PartitionLostEvent;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.ScheduledTaskHandler;
import com.hazelcast.scheduledexecutor.ScheduledTaskStatistics;
import com.hazelcast.scheduledexecutor.StaleTaskException;
import com.hazelcast.scheduledexecutor.impl.DistributedScheduledExecutorService;
import com.hazelcast.scheduledexecutor.impl.ScheduledTaskStatisticsImpl;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Proxy implementation of {@link IScheduledFuture}.
 *
 * @param <V> the return type of the {@link Callable#call()}
 */
public class ClientScheduledFutureProxy<V>
        extends ClientProxy
        implements IScheduledFuture<V>,
                   HazelcastInstanceAware,
                   PartitionLostListener {

    private static final ClientMessageDecoder IS_DONE_DECODER = new ClientMessageDecoder() {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return ScheduledExecutorIsDoneCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder IS_CANCELLED_DECODER = new ClientMessageDecoder() {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return ScheduledExecutorIsCancelledCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder CANCEL_DECODER = new ClientMessageDecoder() {
        @Override
        public Boolean decodeClientMessage(ClientMessage clientMessage) {
            return ScheduledExecutorCancelCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_STATS_DECODER = new ClientMessageDecoder() {
        @Override
        public ScheduledTaskStatistics decodeClientMessage(ClientMessage clientMessage) {
            ScheduledExecutorGetStatsCodec.ResponseParameters responseParameters =
                    ScheduledExecutorGetStatsCodec.decodeResponse(clientMessage);

            return new ScheduledTaskStatisticsImpl(
                    responseParameters.totalRuns, responseParameters.firstRunStartNanos,
                    responseParameters.lastRunStartNanos, responseParameters.lastRunEndNanos,
                    responseParameters.lastIdleTimeNanos, responseParameters.totalRunTimeNanos,
                    responseParameters.totalIdleTimeNanos);
        }
    };

    private static final ClientMessageDecoder GET_DELAY_DECODER = new ClientMessageDecoder() {
        @Override
        public Long decodeClientMessage(ClientMessage clientMessage) {
            return ScheduledExecutorGetDelayCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder GET_RESULT_DECODER = new ClientMessageDecoder() {
        @Override
        public Object decodeClientMessage(ClientMessage clientMessage) {
            return ScheduledExecutorGetResultCodec.decodeResponse(clientMessage).response;
        }
    };

    private static final ClientMessageDecoder DISPOSE_DECODER = new ClientMessageDecoder() {
        @Override
        public Void decodeClientMessage(ClientMessage clientMessage) {
            return null;
        }
    };

    private HazelcastInstance instance;

    private String partitionLostRegistration;

    private boolean partitionLost;

    private ScheduledTaskHandler handler;

    public ClientScheduledFutureProxy(ScheduledTaskHandler handler, ClientContext context) {
        super(DistributedScheduledExecutorService.SERVICE_NAME, handler.getSchedulerName());
        setContext(context);
        this.handler = handler;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        unRegisterPartitionListenerIfExists();

        this.instance = hazelcastInstance;
        registerPartitionListener();
    }

    @Override
    public ScheduledTaskHandler getHandler() {
        return handler;
    }

    @Override
    public ScheduledTaskStatistics getStats() {
        checkAccessibleHandler();
        checkAccessiblePartition();

        ClientMessage request = ScheduledExecutorGetStatsCodec.encodeRequest(handler);
        return this.<ScheduledTaskStatistics>submitAsync(request, GET_STATS_DECODER).join();
    }

    @Override
    public long getDelay(TimeUnit unit) {
        checkAccessibleHandler();
        checkAccessiblePartition();

        ClientMessage request = ScheduledExecutorGetDelayCodec.encodeRequest(handler, unit);
        return this.<Long>submitAsync(request, GET_DELAY_DECODER).join();
    }

    @Override
    public int compareTo(Delayed o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        checkAccessibleHandler();
        checkAccessiblePartition();

        ClientMessage request = ScheduledExecutorCancelCodec.encodeRequest(handler, mayInterruptIfRunning);
        return this.<Boolean>submitAsync(request, CANCEL_DECODER).join();
    }

    @Override
    public boolean isCancelled() {
        checkAccessibleHandler();
        checkAccessiblePartition();

        ClientMessage request = ScheduledExecutorIsCancelledCodec.encodeRequest(handler);
        return this.<Boolean>submitAsync(request, IS_CANCELLED_DECODER).join();
    }

    @Override
    public boolean isDone() {
        checkAccessibleHandler();
        checkAccessiblePartition();

        ClientMessage request = ScheduledExecutorIsDoneCodec.encodeRequest(handler);
        return this.<Boolean>submitAsync(request, IS_DONE_DECODER).join();
    }

    private InternalCompletableFuture<V> get0() {
        checkAccessibleHandler();
        checkAccessiblePartition();

        ClientMessage request = ScheduledExecutorGetResultCodec.encodeRequest(handler);
        return this.submitAsync(request, GET_RESULT_DECODER);
    }

    @Override
    public V get()
            throws InterruptedException, ExecutionException {
        return this.get0().join();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return this.get0().get(timeout, unit);
    }

    public void dispose() {
        checkAccessibleHandler();
        checkAccessiblePartition();

        unRegisterPartitionListenerIfExists();

        ClientMessage request = ScheduledExecutorDisposeCodec.encodeRequest(handler);
        InternalCompletableFuture future = submitAsync(request, DISPOSE_DECODER);
        handler = null;
        future.join();
    }

    @Override
    public void partitionLost(PartitionLostEvent event) {
        if (handler.getPartitionId() == event.getPartitionId()) {
            unRegisterPartitionListenerIfExists();
            this.partitionLost = true;
        }
    }

    private void registerPartitionListener() {
        this.partitionLostRegistration = this.instance.getPartitionService().addPartitionLostListener(this);
    }

    private void unRegisterPartitionListenerIfExists() {
        if (partitionLostRegistration != null) {
            this.instance.getPartitionService().removePartitionLostListener(this.partitionLostRegistration);
        }
    }

    private void checkAccessiblePartition() {
        if (partitionLost) {
            throw new IllegalStateException(
                    "Partition holding this Scheduled task was lost along with all backups.");
        }
    }

    private void checkAccessibleHandler() {
        if (handler == null) {
            throw new StaleTaskException(
                    "Scheduled task was previously disposed.");
        }
    }

    private <T> ClientDelegatingFuture<T> submitAsync(ClientMessage clientMessage,
                                                ClientMessageDecoder clientMessageDecoder) {
        return invokeOnPartitionAsync(clientMessage, clientMessageDecoder, handler.getPartitionId());
    }

    private  <T> ClientDelegatingFuture<T> invokeOnPartitionAsync(ClientMessage clientMessage,
                                                                  ClientMessageDecoder clientMessageDecoder,
                                                                  int partitionId) {
        SerializationService serializationService = getContext().getSerializationService();

        try {
            final ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage,
                    partitionId).invoke();

            return new ClientDelegatingFuture<T>(future, serializationService, clientMessageDecoder);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
