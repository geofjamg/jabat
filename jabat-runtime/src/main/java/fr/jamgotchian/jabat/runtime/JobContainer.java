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
package fr.jamgotchian.jabat.runtime;

import fr.jamgotchian.jabat.jobxml.JobXml;
import fr.jamgotchian.jabat.jobxml.JobXmlLocator;
import fr.jamgotchian.jabat.jobxml.JobXmlParser;
import fr.jamgotchian.jabat.jobxml.MetaInfJobXmlLocator;
import fr.jamgotchian.jabat.jobxml.model.Job;
import fr.jamgotchian.jabat.runtime.artifact.ArtifactFactory;
import fr.jamgotchian.jabat.runtime.artifact.BatchXml;
import fr.jamgotchian.jabat.runtime.repository.BatchStatus;
import fr.jamgotchian.jabat.runtime.repository.JabatJobExecution;
import fr.jamgotchian.jabat.runtime.repository.JabatJobInstance;
import fr.jamgotchian.jabat.runtime.repository.JobRepository;
import fr.jamgotchian.jabat.runtime.task.TaskManager;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
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
public class JobContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(JobContainer.class);

    private final JobXmlLocator locator = new MetaInfJobXmlLocator();

    private final JobXmlParser parser = new JobXmlParser();

    private final BatchXml batchXml;

    private final ArtifactFactory artifactFactory;

    private final TaskManager taskManager;

    private final JobRepository repository;

    private final Map<Long, JobExecutionContext> executionContexts
            = Collections.synchronizedMap(new HashMap<Long, JobExecutionContext>());

    private final JobExecutionListener executionListener = new JobExecutionListener() {

        @Override
        public void started(JobExecutionContext context) {
            executionContexts.put(context.getJobExecution().getId(), context);
        }

        @Override
        public void finished(JobExecutionContext context) {
            executionContexts.remove(context.getJobExecution().getId());
        }
    };

    JobContainer(BatchXml batchXml, ArtifactFactory artifactFactory,
                 TaskManager taskManager, JobRepository repository) {
        this.batchXml = batchXml;
        this.artifactFactory = artifactFactory;
        this.taskManager = taskManager;
        this.repository = repository;
    }

    public void initialize() throws Exception {
        taskManager.initialize();
    }

    public void shutdown() throws Exception {
        taskManager.shutdown();
    }

    public JobRepository getRepository() {
        return repository;
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
        JobXml jobXml = locator.locate(id);
        if (jobXml == null) {
            throw new NoSuchJobException("Job " + id + " not found");
        }

        Job job = parser.parseJob(jobXml.getInputStream());

        if (job.getFirstChainableNode() == null) {
            throw new JobStartException("The job " + id + " does not contain any step, flow or split");
        }

        // TODO check that the job is not already running

        // create a new job instance
        JabatJobInstance jobInstance = repository.createJobInstance(job);

        // create a job execution
        JabatJobExecution jobExecution = repository.createJobExecution(jobInstance, parameters);

        // start the execution
        JobExecutionContext executionContext
                = new JobExecutionContext(batchXml, artifactFactory, taskManager,
                                          repository, parameters, jobInstance,
                                          jobExecution);
        new JobExecutor(job).execute(executionContext, executionListener);

        return jobInstance.getInstanceId();
    }

    public void stop(long instanceId) throws NoSuchJobInstanceException, JobExecutionNotRunningException {
        JabatJobInstance jobInstance = repository.getJobInstance(instanceId);
        if (jobInstance == null) {
            throw new NoSuchJobInstanceException("Job instance " + instanceId + " not found");
        }

        long executionId = jobInstance.getLastExecutionId();

        JobExecutionContext executionContext = executionContexts.get(executionId);
        if (executionContext == null) {
            throw new JobExecutionNotRunningException("Job execution " + executionId + "is not running");
        }

        // update job and steps status to STOPPING
        executionContext.getJobExecution().setStatus(BatchStatus.STOPPING);

        executionContext.stopRunningSteps();

        executionContext.getJobExecution().setStatus(BatchStatus.STOPPED);
    }
}
