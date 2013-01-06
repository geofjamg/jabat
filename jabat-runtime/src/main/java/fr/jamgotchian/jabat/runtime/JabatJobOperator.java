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

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import javax.batch.runtime.JobExecution;
import javax.batch.runtime.JobExecutionNotRunningException;
import javax.batch.runtime.JobInstance;
import javax.batch.runtime.JobInstanceAlreadyCompleteException;
import javax.batch.runtime.JobOperator;
import javax.batch.runtime.JobRestartException;
import javax.batch.runtime.JobStartException;
import javax.batch.runtime.NoSuchJobException;
import javax.batch.runtime.NoSuchJobExecutionException;
import javax.batch.runtime.NoSuchJobInstanceException;
import javax.batch.runtime.StepExecution;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatJobOperator implements JobOperator {

    private final JobContainer container;

    public JabatJobOperator(JobContainer container) {
        this.container = container;
    }

    @Override
    public Set<String> getJobNames() {
        return container.getJobIds();
    }

    @Override
    public long getJobInstanceCount(String jobName) throws NoSuchJobException {
        return container.getJobInstanceIds(jobName).size();
    }

    @Override
    public List<Long> getJobInstanceIds(String jobName, int start, int count) throws NoSuchJobException {
        return container.getJobInstanceIds(jobName);
    }

    @Override
    public Set<Long> getRunningInstanceIds(String jobName) throws NoSuchJobException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public List<Long> getExecutions(long instanceId) throws NoSuchJobInstanceException {
        return Collections.unmodifiableList(container.getRepository().getJobInstance(instanceId).getExecutionIds());
    }

    @Override
    public Properties getParameters(long executionId) throws NoSuchJobExecutionException {
        return container.getRepository().getJobExecution(executionId).getJobParameters();
    }

    @Override
    public Long start(String job, Properties jobParameters) throws NoSuchJobException, JobStartException {
        return container.start(job, jobParameters);
    }

    @Override
    public Long restart(long instanceId, Properties jobParameters) throws JobInstanceAlreadyCompleteException, NoSuchJobExecutionException, NoSuchJobException, JobRestartException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void stop(long instanceId) throws NoSuchJobInstanceException, JobExecutionNotRunningException {
        container.stop(instanceId);
    }

    @Override
    public JobInstance getJobInstance(long instanceId) throws NoSuchJobInstanceException {
        return container.getRepository().getJobInstance(instanceId);
    }

    @Override
    public List<JobExecution> getJobExecutions(long instanceId) throws NoSuchJobInstanceException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public JobExecution getJobExecution(long executionId) throws NoSuchJobExecutionException {
        return container.getRepository().getJobExecution(executionId);
    }

    @Override
    public StepExecution getStepExecution(long jobExecutionId, long stepExecutionId) {
        return container.getRepository().getStepExecution(stepExecutionId);
    }

}
