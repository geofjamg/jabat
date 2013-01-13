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

import fr.jamgotchian.jabat.runtime.artifact.ArtifactContainer;
import fr.jamgotchian.jabat.runtime.artifact.ArtifactFactory;
import fr.jamgotchian.jabat.runtime.artifact.BatchXml;
import fr.jamgotchian.jabat.runtime.repository.BatchStatus;
import fr.jamgotchian.jabat.runtime.repository.JabatJobExecution;
import fr.jamgotchian.jabat.runtime.repository.JabatJobInstance;
import fr.jamgotchian.jabat.runtime.repository.JabatStepExecution;
import fr.jamgotchian.jabat.runtime.repository.JobRepository;
import fr.jamgotchian.jabat.runtime.task.TaskManager;
import java.util.Collections;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import javax.batch.api.Batchlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
class JobExecutionContext {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionContext.class);

    private final BatchXml batchXml;

    private final ArtifactFactory artifactFactory;

    private final TaskManager taskManager;

    private final JobRepository repository;

    private final Properties jobParameters;

    private final JabatJobInstance jobInstance;

    private final JabatJobExecution jobExecution;

    private final Set<Batchlet> runningBatchlets
            = Collections.synchronizedSet(new HashSet<Batchlet>());

    JobExecutionContext(BatchXml batchXml, ArtifactFactory artifactFactory, TaskManager taskManager,
                        JobRepository repository, Properties jobParameters, JabatJobInstance jobInstance,
                        JabatJobExecution jobExecution) {
        this.batchXml = batchXml;
        this.artifactFactory = artifactFactory;
        this.taskManager = taskManager;
        this.repository = repository;
        this.jobParameters = jobParameters;
        this.jobInstance = jobInstance;
        this.jobExecution = jobExecution;
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

    Properties getJobParameters() {
        return jobParameters;
    }

    JabatJobInstance getJobInstance() {
        return jobInstance;
    }

    JabatJobExecution getJobExecution() {
        return jobExecution;
    }

    ArtifactContainer createArtifactContainer() {
        return new ArtifactContainer(batchXml, artifactFactory, runningBatchlets);
    }

    void stopRunningSteps() {
        // change step execution status to STOPPING
        for (long stepExecutionId : jobExecution.getStepExecutionIds()) {
            JabatStepExecution stepExecution = repository.getStepExecution(stepExecutionId);
            stepExecution.setStatus(BatchStatus.STOPPING);
        }

        // 2 cases:
        //   - chunk steps check status regularly (between each item processing)
        //     and consequently stop on their own
        //   - batchlet steps need to be stopped by calling the stop method

        for (Batchlet b : runningBatchlets) {
            try {
                b.stop();
            } catch(Throwable t) {
                LOGGER.error(t.toString(), t);
            }
        }
    }
}
