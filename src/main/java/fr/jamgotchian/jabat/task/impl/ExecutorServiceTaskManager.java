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
package fr.jamgotchian.jabat.task.impl;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import fr.jamgotchian.jabat.task.TaskManager;
import fr.jamgotchian.jabat.task.TaskResultListener;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ExecutorServiceTaskManager implements TaskManager {

    private final ThreadFactory threadFactory = new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("JABAT-%d")
        .build();

    public ExecutorServiceTaskManager() {
    }

    @Override
    public void submit(Runnable task) {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            executorService.submit(task);
        } finally {
            executorService.shutdown();
        }
    }

    @Override
    public <V> void submitAndWait(Collection<Callable<V>> tasks, int threads, TaskResultListener<V> listener) throws InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(threads, threadFactory);
        try {
            List<Future<V>> futures = executorService.invokeAll(tasks);
            for (Future<V> future : futures) {
                try {
                    V result = future.get();
                    listener.onSuccess(result);
                } catch (ExecutionException e) {
                    listener.onFailure(e.getCause());
                }
            }
        } finally {
            executorService.shutdown();
        }
    }

    @Override
    public void initialize() throws Exception {
    }

    @Override
    public void shutdown() throws Exception {
    }

}
