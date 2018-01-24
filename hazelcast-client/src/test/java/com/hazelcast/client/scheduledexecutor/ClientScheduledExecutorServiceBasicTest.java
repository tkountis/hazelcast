/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.scheduledexecutor;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.scheduledexecutor.IScheduledExecutorService;
import com.hazelcast.scheduledexecutor.IScheduledFuture;
import com.hazelcast.scheduledexecutor.ScheduledExecutorServiceBasicTest;
import com.hazelcast.scheduledexecutor.StaleTaskException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientScheduledExecutorServiceBasicTest
        extends ScheduledExecutorServiceBasicTest {

    private TestHazelcastFactory factory;

    @After
    public void teardown() {
        if (factory != null) {
            factory.terminateAll();
        }
    }

    @Override
    protected HazelcastInstance[] createClusterWithCount(int count) {
        return createClusterWithCount(count, new Config());
    }

    @Override
    protected HazelcastInstance[] createClusterWithCount(int count, Config config) {
        factory = new TestHazelcastFactory();
        HazelcastInstance[] instances = factory.newInstances(config, count);
        waitAllForSafeState(instances);
        return instances;
    }

    @Override
    public IScheduledExecutorService getScheduledExecutor(HazelcastInstance[] instances, String name) {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        return factory.newHazelcastClient(config).getScheduledExecutorService(name);
    }

    @Test
    @Ignore("Never supported feature")
    @Override
    public void schedule_testPartitionLostEvent() {
    }

    @Test
    @Ignore("Never supported feature")
    @Override
    public void scheduleOnMember_testMemberLostEvent() {
    }

    @Test
    public void schedule_andDisconnectOwner_verifyOrpahnedRemoval() throws Exception {
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);

        HazelcastInstance[] instances = createClusterWithCount(2);
        // Create two clients
        HazelcastClientProxy clientA = (HazelcastClientProxy) factory.newHazelcastClient(config);
        HazelcastClientProxy clientB = (HazelcastClientProxy) factory.newHazelcastClient(config);

        // Get the executor proxy from client A & schedule a periodic echo task
        IScheduledExecutorService executorService = clientA.getScheduledExecutorService("Scheduler");
        IScheduledFuture future = executorService.scheduleAtFixedRate(
                new EchoTask("One"), 0, 1, SECONDS);

        // Get the executor proxy from client B & schedule a periodic echo task
        IScheduledExecutorService executorServiceB = clientB.getScheduledExecutorService("Scheduler");
        IScheduledFuture futureB = executorServiceB.scheduleAtFixedRate(
                new EchoTask("Two"), 0, 1, SECONDS);

        sleepSeconds(3);

        // Assert that the number of scheduled tasks is 2 & that both of them are in the correct state
        assertEquals(2, countScheduledTasksOn(executorService));
        assertFalse(future.isDone());
        assertFalse(futureB.isDone());

        // Shutdown client A
        clientA.client.getConnectionManager().getOwnerConnection().close("Testing", null);
        sleepSeconds(15);
        assertEquals(1, instances[0].getClientService().getConnectedClients().size());

        // Assert that the number of tasks is now 1 (removed orphaned)
        assertEquals(1, countScheduledTasksOn(executorServiceB));
        assertFalse(futureB.isDone());

        // Assert that access to the orphaned task gives StaleTaskException
        try {
            future.isDone();
            fail();
        } catch (StaleTaskException ex) {

        }
    }
}
