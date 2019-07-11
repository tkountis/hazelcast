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

package com.hazelcast.map.impl.operation;

import com.hazelcast.internal.partition.DelayedBackupContainer;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.PartitionAwareOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class PutBackupPayloadOperation
        extends Operation
        implements PartitionAwareOperation {

    private long payloadId;
    private Data payload;

    public PutBackupPayloadOperation() {
    }

    public PutBackupPayloadOperation(long payloadId, Data payload) {
        this.payloadId = payloadId;
        this.payload = payload;
    }

    @Override
    public void run() throws Exception {
        InternalPartitionServiceImpl partitionService = (InternalPartitionServiceImpl) getNodeEngine().getPartitionService();
        DelayedBackupContainer delayedBackupContainer = partitionService.getDelayedBackupContainer(getPartitionId());

        for (Backup backup : delayedBackupContainer.handlePayload(payloadId, payload)) {
            backup.doRunBackup();
            backup.afterRun();
        }
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        throw new UnsupportedEncodingException();
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        throw new UnsupportedEncodingException();
    }
}
