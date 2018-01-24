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

import java.util.concurrent.ScheduledFuture;

/**
 * A delayed result-bearing action that can be cancelled.
 * Usually a scheduled future is the result of scheduling
 * a task with a {@link IScheduledExecutorService}.
 *
 * Enhances the default {@link ScheduledFuture} API with support
 * of statistics and time measurement info, through {@link ScheduledTaskStatistics}
 *
 * @param <V> The result type returned by this Future
 */
public interface IScheduledFuture<V>
        extends ScheduledFuture<V>, IFuture<V> {

    /**
     * Returns the scheduled future resource handler.
     * Can be used to re-acquire control of the {@link IScheduledFuture} using the
     * {@link IScheduledExecutorService#getScheduledFuture(ScheduledTaskHandler)}
     *
     * @return An instance of {@link ScheduledTaskHandler}, a resource handler for this scheduled future.
     */
    ScheduledTaskHandler getHandler();

    /**
     * Returns the statistics and time measurement info of the execution of this scheduled future
     * in the {@link IScheduledExecutorService} it was scheduled.
     *
     * @return An instance of {@link ScheduledTaskStatistics}, holding all stas and measurements
     */
    ScheduledTaskStatistics getStats();

}
