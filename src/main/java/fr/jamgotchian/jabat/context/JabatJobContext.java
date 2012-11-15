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
package fr.jamgotchian.jabat.context;

import fr.jamgotchian.jabat.job.Job;
import fr.jamgotchian.jabat.repository.JabatJobExecution;
import fr.jamgotchian.jabat.repository.JabatJobInstance;
import java.util.List;
import java.util.Properties;
import javax.batch.runtime.context.FlowContext;
import javax.batch.runtime.context.JobContext;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatJobContext<T> implements JobContext<T> {

    private final Job job;

    private final JabatJobInstance jobInstance;

    private final JabatJobExecution jobExecution;

    private T transientUserData;

    private String exitStatus;

    public JabatJobContext(Job job, JabatJobInstance jobInstance, JabatJobExecution jobExecution) {
        this.job = job;
        this.jobInstance = jobInstance;
        this.jobExecution = jobExecution;
    }

    @Override
    public String getId() {
        return job.getId();
    }

    @Override
    public long getInstanceId() {
        return jobInstance.getInstanceId();
    }

    @Override
    public long getExecutionId() {
        return jobExecution.getId();
    }

    @Override
    public Properties getProperties() {
        return job.getProperties();
    }

    @Override
    public String getBatchStatus() {
        return jobExecution.getStatus();
    }

    @Override
    public T getTransientUserData() {
        return transientUserData;
    }

    @Override
    public void setTransientUserData(T data) {
        this.transientUserData = data;
    }

    @Override
    public String getExitStatus() {
        return exitStatus;
    }

    @Override
    public void setExitStatus(String exitStatus) {
        this.exitStatus = exitStatus;
    }

    @Override
    public List<FlowContext<T>> getBatchContexts() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
