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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ExecutorServiceTaskManager implements TaskManager {

    private final ExecutorService executor = Executors.newCachedThreadPool();

    public ExecutorServiceTaskManager() {
    }

    @Override
    public void submit(Runnable task) {
        executor.submit(task);
    }

    @Override
    public void initialize() throws Exception {
    }

    @Override
    public void shutdownAndWaitForTermination() throws Exception {
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        executor.shutdownNow();
    }

}
