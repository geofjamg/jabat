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
package fr.jamgotchian.jabat.runtime.repository.impl;

import fr.jamgotchian.jabat.jobxml.model.Job;
import fr.jamgotchian.jabat.jobxml.model.Step;
import fr.jamgotchian.jabat.runtime.repository.JabatJobExecution;
import fr.jamgotchian.jabat.runtime.repository.JabatJobInstance;
import fr.jamgotchian.jabat.runtime.repository.JabatStepExecution;
import fr.jamgotchian.jabat.runtime.repository.JobRepository;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JobRepositoryImpl implements JobRepository {

    private final Map<String, List<Long>> jobs = new HashMap<String, List<Long>>();

    private final Map<Long, JabatJobInstance> jobInstances = new HashMap<Long, JabatJobInstance>();

    private final Map<Long, JabatJobExecution> jobExecutions = new HashMap<Long, JabatJobExecution>();

    private final Map<String, List<Long>> steps = new HashMap<String, List<Long>>();

    private final Map<Long, JabatStepExecution> stepExecutions = new HashMap<Long, JabatStepExecution>();

    private long nextJobInstanceId = 0;

    private long nextJobExecutionId = 0;

    private long nextStepExecutionId = 0;

    @Override
    public JabatJobInstance createJobInstance(Job job) {
        long jobInstanceId = nextJobInstanceId++;
        JabatJobInstance jobInstance = new JabatJobInstanceImpl(job.getId(), jobInstanceId);
        jobInstances.put(jobInstanceId, jobInstance);
        List<Long> instanceIds = jobs.get(job.getId());
        if (instanceIds == null) {
            instanceIds = new ArrayList<Long>(1);
            jobs.put(job.getId(), instanceIds);
        }
        instanceIds.add(jobInstanceId);
        return jobInstance;
    }

    @Override
    public JabatJobExecution createJobExecution(JabatJobInstance jobInstance) {
        long jobExecutionId = nextJobExecutionId++;
        JabatJobExecution jobExecution = new JabatJobExecutionImpl(jobExecutionId);
        jobExecutions.put(jobExecutionId, jobExecution);
        jobInstance.getExecutionIds().add(jobExecutionId);
        return jobExecution;
    }

    @Override
    public JabatStepExecution createStepExecution(Step step, JabatJobExecution jobExecution) {
        long stepExecutionId = nextStepExecutionId++;
        JabatStepExecution stepExecution = new JabatStepExecutionImpl(stepExecutionId);
        stepExecutions.put(stepExecutionId, stepExecution);
        List<Long> stepExecutionIds = steps.get(step.getId());
        if (stepExecutionIds == null) {
            stepExecutionIds = new ArrayList<Long>(1);
            steps.put(step.getId(), stepExecutionIds);
        }
        stepExecutionIds.add(stepExecutionId);
        jobExecution.getStepExecutionIds().add(stepExecutionId);
        return stepExecution;
    }

    @Override
    public Set<String> getJobIds() {
        return jobs.keySet();
    }

    @Override
    public List<Long> getJobInstanceIds(String id) {
        return jobs.get(id);
    }

    @Override
    public JabatJobInstance getJobInstance(long id) {
        return jobInstances.get(id);
    }

    @Override
    public JabatJobExecution getJobExecution(long id) {
        return jobExecutions.get(id);
    }

    @Override
    public JabatStepExecution getStepExecution(long id) {
        return stepExecutions.get(id);
    }

}
