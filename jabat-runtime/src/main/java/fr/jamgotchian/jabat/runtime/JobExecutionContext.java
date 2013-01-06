/*
 * Copyright 2013 Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>.
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
package fr.jamgotchian.jabat.runtime;

import com.google.common.collect.Multimap;
import fr.jamgotchian.jabat.runtime.artifact.ArtifactFactory;
import fr.jamgotchian.jabat.runtime.repository.JobRepository;
import fr.jamgotchian.jabat.runtime.task.TaskManager;
import javax.batch.api.Batchlet;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
class JobExecutionContext {

    private final TaskManager taskManager;

    private final ArtifactFactory artifactFactory;

    private final JobRepository repository;

    private final Multimap<Long, Batchlet> runningBatchlets;

    JobExecutionContext(TaskManager taskManager, ArtifactFactory artifactFactory,
                        JobRepository repository, Multimap<Long, Batchlet> runningBatchlets) {
        this.taskManager = taskManager;
        this.artifactFactory = artifactFactory;
        this.repository = repository;
        this.runningBatchlets = runningBatchlets;
    }

    TaskManager getTaskManager() {
        return taskManager;
    }

    ArtifactFactory getArtifactFactory() {
        return artifactFactory;
    }

    JobRepository getRepository() {
        return repository;
    }

    Multimap<Long, Batchlet> getRunningBatchlets() {
        return runningBatchlets;
    }

}
