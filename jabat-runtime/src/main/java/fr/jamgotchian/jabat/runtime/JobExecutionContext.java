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
import fr.jamgotchian.jabat.runtime.artifact.BatchXml;
import fr.jamgotchian.jabat.runtime.repository.JobRepository;
import fr.jamgotchian.jabat.runtime.task.TaskManager;
import javax.batch.api.Batchlet;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
class JobExecutionContext {

    private final BatchXml batchXml;

    private final ArtifactFactory artifactFactory;

    private final TaskManager taskManager;

    private final JobRepository repository;

    private final Multimap<Long, Batchlet> runningBatchlets;

    JobExecutionContext(BatchXml batchXml, ArtifactFactory artifactFactory, TaskManager taskManager,
                        JobRepository repository, Multimap<Long, Batchlet> runningBatchlets) {
        this.batchXml = batchXml;
        this.artifactFactory = artifactFactory;
        this.taskManager = taskManager;
        this.repository = repository;
        this.runningBatchlets = runningBatchlets;
    }

    BatchXml getBatchXml() {
        return batchXml;
    }

    ArtifactFactory getArtifactFactory() {
        return artifactFactory;
    }

    TaskManager getTaskManager() {
        return taskManager;
    }

    JobRepository getRepository() {
        return repository;
    }

    Multimap<Long, Batchlet> getRunningBatchlets() {
        return runningBatchlets;
    }

}
