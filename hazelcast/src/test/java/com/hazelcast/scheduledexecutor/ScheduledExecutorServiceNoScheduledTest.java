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

package com.hazelcast.scheduledexecutor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICountDownLatch;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.concurrent.TimeUnit;

import static com.hazelcast.scheduledexecutor.TaskUtils.named;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ScheduledExecutorServiceNoScheduledTest
        extends ScheduledExecutorServiceTestSupport {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void execute_verifyNoLeftovers() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);
        final IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        ICountDownLatch latch = instances[0].getCountDownLatch("TestMe");

        latch.trySetCount(1);
        assertEquals(0, countScheduledTasksOn(executorService));

        executorService.execute(named("MyTask1", new SleepyRunnableTask("TestMe", 5)));
        assertEquals(1, countScheduledTasksOn(executorService));

        latch.await(10, TimeUnit.SECONDS);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(0, countScheduledTasksOn(executorService));
            }
        });
    }

    @Test
    public void submit() throws Exception {
        HazelcastInstance[] instances = createClusterWithCount(2);
        IScheduledExecutorService executorService = getScheduledExecutor(instances, "s");
        ICountDownLatch latch = instances[0].getCountDownLatch("TestMe");

        latch.trySetCount(1);
        assertEquals(0, countScheduledTasksOn(executorService));

        IFuture<Double> future = executorService.submit(named("MyTask1",
                new ICountdownLatchCallableTask("TestMe", 10000)));

        assertEquals(1, countScheduledTasksOn(executorService));

        latch.await(15, TimeUnit.SECONDS);
        assertEquals(77 * 2.2, future.get(), 0);
        assertEquals(1, countScheduledTasksOn(executorService));

        future.dispose();
        assertEquals(0, countScheduledTasksOn(executorService));
    }

}
