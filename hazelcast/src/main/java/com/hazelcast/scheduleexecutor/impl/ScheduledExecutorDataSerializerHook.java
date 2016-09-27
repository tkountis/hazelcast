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

package com.hazelcast.scheduleexecutor.impl;

import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.scheduleexecutor.impl.operations.CancelTaskBackupOperation;
import com.hazelcast.scheduleexecutor.impl.operations.CancelTaskOperation;
import com.hazelcast.scheduleexecutor.impl.operations.CompareToOperation;
import com.hazelcast.scheduleexecutor.impl.operations.DestroyTaskOperation;
import com.hazelcast.scheduleexecutor.impl.operations.GetDelayOperation;
import com.hazelcast.scheduleexecutor.impl.operations.GetStatisticsOperation;
import com.hazelcast.scheduleexecutor.impl.operations.IsCanceledOperation;
import com.hazelcast.scheduleexecutor.impl.operations.IsDoneOperation;
import com.hazelcast.scheduleexecutor.impl.operations.ReplicationOperation;
import com.hazelcast.scheduleexecutor.impl.operations.ScheduleTaskBackupOperation;
import com.hazelcast.scheduleexecutor.impl.operations.ScheduleTaskOperation;
import com.hazelcast.scheduleexecutor.impl.operations.SyncStateOperation;

import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SCHEDULED_EXECUTOR_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.SCHEDULED_EXECUTOR_DS_FACTORY_ID;

public class ScheduledExecutorDataSerializerHook implements DataSerializerHook {

    public static final int F_ID = FactoryIdHelper.getFactoryId(
            SCHEDULED_EXECUTOR_DS_FACTORY, SCHEDULED_EXECUTOR_DS_FACTORY_ID);

    public static final int TASK_DESCRIPTOR = 0;
    public static final int RUNNABLE_DEFINITION = 19;

    public static final int SCHEDULE_OP = 1;
    public static final int SCHEDULE_BACKUP_OP = 2;

    public static final int SCHEDULED_FUTURE = 7;

    public static final int CANCEL_OP = 8;
    public static final int CANCEL_BACKUP_OP = 9;

    public static final int GET_DELAY_OP = 10;
    public static final int COMPARE_TO_OP = 11;
    public static final int IS_DONE_OP = 12;
    public static final int IS_CANCELED_OP = 13;
    public static final int GET_STATS_OP = 14;
    public static final int TASK_STATS = 15;

    public static final int SYNC_STATE_OP = 16;
    public static final int REPLICATION = 17;

    public static final int DESTROY_TASK_OP = 18;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return new DataSerializableFactory() {
            @Override
            public IdentifiedDataSerializable create(int typeId) {
                switch (typeId) {
                    case TASK_DESCRIPTOR:
                        return new ScheduledTaskDescriptor();
                    case RUNNABLE_DEFINITION:
                        return new RunnableDefinition();
                    case SCHEDULED_FUTURE:
                        return new ScheduledFutureProxy();
                    case GET_DELAY_OP:
                        return new GetDelayOperation();
                    case CANCEL_OP:
                        return new CancelTaskOperation();
                    case CANCEL_BACKUP_OP:
                        return new CancelTaskBackupOperation();
                    case SCHEDULE_OP:
                        return new ScheduleTaskOperation();
                    case DESTROY_TASK_OP:
                        return new DestroyTaskOperation();
                    case COMPARE_TO_OP:
                        return new CompareToOperation();
                    case IS_DONE_OP:
                        return new IsDoneOperation();
                    case IS_CANCELED_OP:
                        return new IsCanceledOperation();
                    case TASK_STATS:
                        return new ScheduledTaskStatisticsImpl();
                    case GET_STATS_OP:
                        return new GetStatisticsOperation();
                    case SCHEDULE_BACKUP_OP:
                        return new ScheduleTaskBackupOperation();
                    case SYNC_STATE_OP:
                        return new SyncStateOperation();
                    case REPLICATION:
                        return new ReplicationOperation();
                    default:
                        return null;
                }
            }
        };
    }
}