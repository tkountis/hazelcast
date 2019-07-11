package com.hazelcast.internal.partition;

import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.operationservice.BackupOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.util.collection.Long2ObjectHashMap;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

public class DelayedBackupContainer {

    private final Queue<Backup> delayedBackups = new ArrayDeque<>(1000);
    private final Long2ObjectHashMap<Data> payloads = new Long2ObjectHashMap<>();

    public DelayedBackupContainer() {
    }

    public boolean delayIfMissingPayloads(Backup backup) {
        BackupOperation operation = (BackupOperation) backup.getBackupOp();

        if (operation.getPayloadId() == 0) {
            return false;
        }

        Data payload = payloads.get(operation.getPayloadId());
        if (payload == null) {
            delayedBackups.add(backup);
            return true;
        }

        operation.injectPayload(payload);
        return false;
    }

    public List<Backup> handlePayload(long payloadId, Data payload) {
        payloads.put(payloadId, payload);

        List<Backup> readyToRun = null;

        while (true) {
            Backup backup = delayedBackups.peek();
            if (backup == null) {
                break;
            }

            BackupOperation operation = (BackupOperation) backup.getBackupOp();
            Object otherPayload = payloads.get(operation.getPayloadId());
            if (otherPayload == null) {
                break;
            }

            operation.injectPayload(payload);

            if (readyToRun == null) {
                readyToRun = new ArrayList<>();
            }

            readyToRun.add(backup);
        }

        return readyToRun != null ? readyToRun : Collections.emptyList();
    }

}
