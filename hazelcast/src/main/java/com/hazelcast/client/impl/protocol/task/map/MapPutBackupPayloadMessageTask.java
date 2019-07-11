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

package com.hazelcast.client.impl.protocol.task.map;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapPutBackupPayloadCodec;
import com.hazelcast.client.impl.protocol.codec.MapPutCodec;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.operation.PutBackupPayloadOperation;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.security.Permission;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.map.impl.recordstore.RecordStore.DEFAULT_MAX_IDLE;

public class MapPutBackupPayloadMessageTask
        extends AbstractMapPartitionMessageTask<MapPutBackupPayloadCodec.RequestParameters> {

    public MapPutBackupPayloadMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    @Override
    protected Operation prepareOperation() {
        return new PutBackupPayloadOperation(parameters.id, parameters.value);
    }

    @Override
    protected MapPutBackupPayloadCodec.RequestParameters decodeClientMessage(ClientMessage clientMessage) {
        return MapPutBackupPayloadCodec.decodeRequest(clientMessage);
    }

    @Override
    protected ClientMessage encodeResponse(Object response) {
        return MapPutCodec.encodeResponse(serializationService.toData(response));
    }

    public String getServiceName() {
        return MapService.SERVICE_NAME;
    }

    @Override
    public Permission getRequiredPermission() {
        return null;
    }

    @Override
    public String getDistributedObjectName() {
        return null;
    }

    @Override
    public String getMethodName() {
        return "putBackupPayload";
    }

    @Override
    public Object[] getParameters() {
        return new Object[]{};
    }
}
