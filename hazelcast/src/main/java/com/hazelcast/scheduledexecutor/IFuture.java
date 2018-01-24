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

import java.util.concurrent.Future;

/**
 * A delayed result-bearing action that can be cancelled.
 * Usually a future is the result of submitting
 * a task with a {@link IScheduledExecutorService}.
 *
 * @param <V> The result type returned by this Future
 */
public interface IFuture<V>
        extends Future<V> {

    /**
     * Used to destroy the instance of the {@link IFuture} in the scheduled executor.
     * Once the instance is destroyed, any subsequent action on the {@link IFuture} will
     * fail with an {@link IllegalStateException}
     */
    void dispose();

    /**
     * Attempts to cancel further scheduling of this task.  This attempt will
     * fail if the task has already completed, has already been cancelled,
     * or could not be cancelled for some other reason. If successful,
     * and this task has not started when {@code cancel} is called,
     * this task should never run.
     *
     * <p><b>Warning: </b> This cancel will attempt to interrupt the running
     * thread if the task is already in progress, without guaranting success.</p>
     *
     * <p>After this method returns, subsequent calls to {@link #isDone} will
     * always return {@code true}.  Subsequent calls to {@link #isCancelled}
     * will always return {@code true} if this method returned {@code true}.
     *
     * @param mayInterruptIfRunning is throwing {@link UnsupportedOperationException}
     * @return {@code false} if the task could not be cancelled,
     * typically because it has already completed normally;
     * {@code true
     */
    boolean cancel(boolean mayInterruptIfRunning);

}
