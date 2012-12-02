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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import fr.jamgotchian.jabat.artifact.ArtifactFactory;
import fr.jamgotchian.jabat.config.Configuration;
import fr.jamgotchian.jabat.job.Job;
import fr.jamgotchian.jabat.job.JobXmlLoader;
import fr.jamgotchian.jabat.repository.BatchStatus;
import fr.jamgotchian.jabat.repository.JabatJobExecution;
import fr.jamgotchian.jabat.repository.JabatJobInstance;
import fr.jamgotchian.jabat.repository.JabatStepExecution;
import fr.jamgotchian.jabat.repository.JobRepository;
import fr.jamgotchian.jabat.task.TaskManager;
import fr.jamgotchian.jabat.util.JabatException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.batch.api.Batchlet;
import javax.batch.runtime.JobExecutionNotRunningException;
import javax.batch.runtime.JobStartException;
import javax.batch.runtime.NoSuchJobException;
import javax.batch.runtime.NoSuchJobInstanceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobManager.class);

    private final JobXmlLoader loader = new JobXmlLoader();

    private final TaskManager taskManager;

    private final ArtifactFactory artifactFactory;

    private final JobRepository repository;

    private final Multimap<Long, Batchlet> runningBatchlets
            = Multimaps.synchronizedMultimap(HashMultimap.<Long, Batchlet>create());

    public JobManager() {
        Configuration cfg = new Configuration();
        Class<?> taskManagerClass = cfg.getTaskManagerClass();
        Class<?> artifactFactoryClass = cfg.getArtifactFactoryClass();
        Class<?> jobRepositoryClass = cfg.getJobRepositoryClass();
        try {
            taskManager = (TaskManager) taskManagerClass.newInstance();
            artifactFactory = (ArtifactFactory) artifactFactoryClass.newInstance();
            repository = (JobRepository) jobRepositoryClass.newInstance();
        } catch(ReflectiveOperationException e) {
            throw new JabatException(e);
        }
    }

    public void initialize() throws Exception {
        taskManager.initialize();
        artifactFactory.initialize();
    }

    public void shutdown() throws Exception {
        taskManager.shutdown();
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

    Multimap<Long, Batchlet> getRunningBatchlets() {
        return runningBatchlets;
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
        job.accept(new JobInstanceExecutor(this, jobInstance, parameters), null);

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
        jobExecution.setStatus(BatchStatus.STOPPING);
        for (long stepExecutionId : jobExecution.getStepExecutionIds()) {
            JabatStepExecution stepExecution = repository.getStepExecution(stepExecutionId);
            stepExecution.setStatus(BatchStatus.STOPPING);
        }

        for (long stepExecutionId : jobExecution.getStepExecutionIds()) {
            JabatStepExecution stepExecution = repository.getStepExecution(stepExecutionId);
            if (BatchStatus.STARTED.name().equals(stepExecution.getStatus())) {
                Collection<Batchlet> batchlets = runningBatchlets.get(stepExecution.getId());
                for (Batchlet batchlet : batchlets) {
                    try {
                        batchlet.stop();
                        stepExecution.setStatus(BatchStatus.STOPPED);
                    } catch(Throwable t) {
                        LOGGER.error(t.toString(), t);
                    }
                }
            }
        }
        jobExecution.setStatus(BatchStatus.STOPPED);
    }
}
