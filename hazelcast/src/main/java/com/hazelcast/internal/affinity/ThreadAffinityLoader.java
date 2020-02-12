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

package com.hazelcast.internal.affinity;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.properties.GroupProperty;

import static com.hazelcast.internal.affinity.ThreadAffinity.Group.IO;
import static com.hazelcast.internal.affinity.ThreadAffinity.Group.PARTITION_THREAD;
import static com.hazelcast.internal.affinity.ThreadAffinityParamsHelper.isAffinityEnabled;
import static com.hazelcast.util.EmptyStatement.ignore;
import static com.hazelcast.util.OsHelper.OS;
import static com.hazelcast.util.OsHelper.isUnixFamily;
import static java.util.Collections.disjoint;

@SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
public final class ThreadAffinityLoader {

    private ThreadAffinityLoader() {
    }

    public static AffinitySupportExtention loadExtention(ILogger logger) {
        if (!isAffinityEnabled()) {
            return null;
        }

        if (!isUnixFamily()) {
            throw new IllegalStateException("Thread affinity not supported on this platform: " + OS);
        }

        logger.info("Thread affinity option detected, checking for dependencies");

        try {
            // Check if dependency OpenHFT Affinity is present in classpath
            Class.forName("net.openhft.affinity.AffinityLock");
        } catch (ClassNotFoundException e) {
            logger.warning("Dependency net.openhft:affinity not found in classpath. Affinity support is disabled.");
            return null;
        }

        try {
            // Check if JNA is available in the classpath
            Class.forName("com.sun.jna.Platform");
        } catch (ClassNotFoundException e) {
            logger.warning("Dependency net.java.dev.jna:jna* not found in classpath. Affinity support is disabled.");
        }

        if (!disjoint(ThreadAffinityParamsHelper.getCoreIds(IO), ThreadAffinityParamsHelper.getCoreIds(PARTITION_THREAD))) {
            logger.warning("Affinity assignments for the different affinity groups have some cores in common (shared).");
        }

        failFastChecks();

        logger.info("Thread affinity dependencies available, support enabled");
        return new AffinitySupportExtention(logger);
    }

    private static void failFastChecks() {
        // Available resources conflicts
        int totalAvailableCores = Runtime.getRuntime().availableProcessors();
        int partitionOpCores = ThreadAffinityParamsHelper.countCores(PARTITION_THREAD);
        if (partitionOpCores > totalAvailableCores) {
            throw new IllegalStateException("Thread affinity core count for " + PARTITION_THREAD + " is set to high, "
                    + "more than the available core count on the system " + totalAvailableCores);
        }

        int ioCores = ThreadAffinityParamsHelper.countCores(IO);
        if (ioCores > totalAvailableCores) {
            throw new IllegalStateException("Thread affinity core count for " + IO + " is set to high, "
                    + "more than the available core count on the system " + totalAvailableCores);
        }

        if (ioCores % 2 != 0) {
            throw new IllegalStateException("Thread affinity core count for " + IO + " is not an even number");
        }

        if (partitionOpCores + ioCores > totalAvailableCores) {
            throw new IllegalStateException("Thread affinity core sum for all affinity groups is to high, "
                    + "more than the available core count on the system " + totalAvailableCores);
        }

        // Other configuration conflicts
        try {
            int partOpCountSystemProperty = Integer.parseInt(GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName());
            if (partOpCountSystemProperty != partitionOpCores) {
                throw new IllegalStateException("Thread affinity core count for " + PARTITION_THREAD
                        + " conflicts with system property " + GroupProperty.PARTITION_OPERATION_THREAD_COUNT.getName());
            }
        } catch (NumberFormatException ex) {
            ignore(ex);
        }

        try {
            int ioCountSystemProperty = Integer.parseInt(GroupProperty.IO_THREAD_COUNT.getName());
            if (ioCountSystemProperty != ioCores) {
                throw new IllegalStateException("Thread affinity core count for " + IO + " conflicts with system property "
                        + GroupProperty.IO_THREAD_COUNT.getName());
            }
        } catch (NumberFormatException ex) {
            ignore(ex);
        }

        try {
            int ioCountSystemProperty = Integer.parseInt(GroupProperty.IO_INPUT_THREAD_COUNT.getName());
            if (ioCountSystemProperty != ioCores / 2) {
                throw new IllegalStateException("Thread affinity core count for " + IO + " conflicts with system property "
                        + GroupProperty.IO_INPUT_THREAD_COUNT.getName());
            }
        } catch (NumberFormatException ex) {
            ignore(ex);
        }

        try {
            int ioCountSystemProperty = Integer.parseInt(GroupProperty.IO_OUTPUT_THREAD_COUNT.getName());
            if (ioCountSystemProperty != ioCores / 2) {
                throw new IllegalStateException("Thread affinity core count for " + IO + " conflicts with system property "
                        + GroupProperty.IO_OUTPUT_THREAD_COUNT.getName());
            }
        } catch (NumberFormatException ex) {
            ignore(ex);
        }
    }

}
