/*
 * Copyright 2012 Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fr.jamgotchian.jabat.task;

import java.util.Collection;
import java.util.concurrent.Callable;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public interface TaskManager {

    /**
     * Submit a task for an asynchronous run.
     *
     * @param task the task to run
     */
    void submit(Runnable task);

    /**
     * Submit a list of task and wait until all tasks complete.
     *
     * @param <V>
     * @param tasks list of task to run
     * @param threads number of thread
     * @param listener listener to be notified when a task succeeds or fails
     * @throws InterruptedException
     */
    <V> void submitAndWait(Collection<Callable<V>> tasks, int threads, TaskResultListener<V> listener) throws InterruptedException;

    /**
     * Initialize the task manager, should be called before submiting a task.
     *
     * @throws Exception
     */
    void initialize() throws Exception;

    /**
     * Shutdown the task manager.
     *
     * @throws Exception
     */
    void shutdown() throws Exception;

}
