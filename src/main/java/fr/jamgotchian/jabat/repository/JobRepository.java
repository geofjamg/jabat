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
package fr.jamgotchian.jabat.repository;

import fr.jamgotchian.jabat.job.Job;
import fr.jamgotchian.jabat.job.StepNode;
import java.util.HashMap;
import java.util.Map;

/**
 * A job repository contains meta data about currently running processes.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobRepository {

    private final Map<Long, JabatJobInstance> jobInstances = new HashMap<Long, JabatJobInstance>();

    private final Map<Long, JabatJobExecution> jobExecutions = new HashMap<Long, JabatJobExecution>();

    private final Map<Long, JabatStepExecution> stepExecutions = new HashMap<Long, JabatStepExecution>();

    private long nextJobInstanceId = 0;

    private long nextJobExecutionId = 0;

    private long nextStepExecutionId = 0;

    public JabatJobInstance createJobInstance(Job job) {
        long jobInstanceId = nextJobInstanceId++;
        JabatJobInstance jobInstance = new JabatJobInstance(job.getId(), jobInstanceId);
        jobInstances.put(jobInstanceId, jobInstance);
        job.getInstanceIds().add(jobInstanceId);
        return jobInstance;
    }

    public JabatJobExecution createJobExecution(JabatJobInstance jobInstance) {
        long jobExecutionId = nextJobExecutionId++;
        JabatJobExecution jobExecution = new JabatJobExecution(jobExecutionId);
        jobExecutions.put(jobExecutionId, jobExecution);
        jobInstance.getExecutionIds().add(jobExecutionId);
        return jobExecution;
    }

    public JabatStepExecution createStepExecution(StepNode step, JabatJobExecution jobExecution) {
        long stepExecutionId = nextStepExecutionId++;
        JabatStepExecution stepExecution = new JabatStepExecution(stepExecutionId);
        stepExecutions.put(stepExecutionId, stepExecution);
        step.getStepExecutionIds().add(stepExecutionId);
        jobExecution.getStepExecutionIds().add(stepExecutionId);
        return stepExecution;
    }

    public JabatJobInstance getJobInstance(long id) {
        return jobInstances.get(id);
    }

    public JabatJobExecution getJobExecution(long id) {
        return jobExecutions.get(id);
    }

    public JabatStepExecution getStepExecution(long id) {
        return stepExecutions.get(id);
    }

}
