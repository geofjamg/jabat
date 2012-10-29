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
package fr.jamgotchian.jabat;

import fr.jamgotchian.jabat.job.JobXmlLoader;
import fr.jamgotchian.jabat.repository.JobRepository;
import fr.jamgotchian.jabat.repository.JabatJobInstance;
import fr.jamgotchian.jabat.repository.JabatStepExecution;
import fr.jamgotchian.jabat.repository.Status;
import fr.jamgotchian.jabat.repository.JabatJobExecution;
import fr.jamgotchian.jabat.job.Job;
import fr.jamgotchian.jabat.artifact.BatchletArtifact;
import fr.jamgotchian.jabat.task.TaskManager;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.batch.runtime.JobExecutionNotRunningException;
import javax.batch.runtime.JobStartException;
import javax.batch.runtime.NoSuchJobException;
import javax.batch.runtime.NoSuchJobInstanceException;
import javax.batch.spi.ArtifactFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);

    private final JobXmlLoader loader = new JobXmlLoader();

    private final JobRepository repository = new JobRepository();

    private final TaskManager taskManager;

    private final ArtifactFactory artifactFactory;

    public JobManager(TaskManager taskManager, ArtifactFactory artifactFactory) {
        this.taskManager = taskManager;
        this.artifactFactory = artifactFactory;
    }

    public void initialize() throws Exception {
        taskManager.initialize();
        artifactFactory.initialize();
    }

    public void shutdown() throws Exception {
        taskManager.shutdownAndWaitForTermination();
    }

    JobRepository getRepository() {
        return repository;
    }

    TaskManager getTaskManager() {
        return taskManager;
    }

    ArtifactFactory getArtifactFactory() {
        return artifactFactory;
    }

    public Set<String> getJobIds() {
        return Collections.unmodifiableSet(repository.getJobIds());
    }

    public List<Long> getJobInstanceIds(String jobId) throws NoSuchJobException {
        List<Long> jobInstanceIds = repository.getJobInstanceIds(jobId);
        if (jobInstanceIds == null) {
            throw new NoSuchJobException("Job " + jobId + " not found");
        }
        return Collections.unmodifiableList(jobInstanceIds);
    }

    public long start(String id, Properties parameters) throws NoSuchJobException, JobStartException {
        Job job = loader.load(id, parameters);

        if (job.getFirstChainableNode() == null) {
            throw new JobStartException("The job " + id + " does not contain any step, flow or split");
        }

        // TODO check that the job is not already running

        // create a new job instance
        JabatJobInstance jobInstance = repository.createJobInstance(job);

        // start the execution
        job.accept(new JobInstanceExecutor(this, jobInstance));

        return jobInstance.getInstanceId();
    }

    public void stop(long instanceId) throws NoSuchJobInstanceException, JobExecutionNotRunningException {
        JabatJobInstance jobInstance = repository.getJobInstance(instanceId);
        if (jobInstance == null) {
            throw new NoSuchJobInstanceException("Job instance " + instanceId + " not found");
        }

        long executionId = jobInstance.getLastExecutionId();
        JabatJobExecution jobExecution = repository.getJobExecution(executionId);

        // TODO check the instance is running

        // update job and steps status to STOPPING
        jobExecution.setStatus(Status.STOPPING);
        for (long stepExecutionId : jobExecution.getStepExecutionIds()) {
            JabatStepExecution stepExecution = repository.getStepExecution(stepExecutionId);
            stepExecution.setStatus(Status.STOPPING);
        }

        for (long stepExecutionId : jobExecution.getStepExecutionIds()) {
            JabatStepExecution stepExecution = repository.getStepExecution(stepExecutionId);
            // TODO only stop running check execution
            BatchletArtifact artifact = stepExecution.getBatchletArtifact();
            if (artifact != null) {
                try {
                    artifact.stop();
                    stepExecution.setStatus(Status.STOPPED);
                } catch(Exception e) {
                    LOGGER.error(e.toString(), e);
                }
            }
        }
        jobExecution.setStatus(Status.STOPPED);
    }
}
