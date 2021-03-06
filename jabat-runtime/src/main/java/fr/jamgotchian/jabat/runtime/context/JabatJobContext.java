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
package fr.jamgotchian.jabat.runtime.context;

import fr.jamgotchian.jabat.jobxml.model.Job;
import fr.jamgotchian.jabat.runtime.repository.JabatJobExecution;
import fr.jamgotchian.jabat.runtime.repository.JabatJobInstance;
import java.util.Properties;
import javax.batch.runtime.context.JobContext;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatJobContext extends JabatBatchContext<Job, Object> implements JobContext<Object> {

    private final JabatJobInstance jobInstance;

    private final JabatJobExecution jobExecution;

    private Properties properties;

    public JabatJobContext(Job job, JabatJobInstance jobInstance, JabatJobExecution jobExecution) {
        super(job);
        this.jobInstance = jobInstance;
        this.jobExecution = jobExecution;
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
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    @Override
    public String getBatchStatus() {
        return jobExecution.getStatus();
    }

    @Override
    public String getExitStatus() {
        return jobExecution.getExitStatus();
    }

    @Override
    public void setExitStatus(String exitStatus) {
        jobExecution.setExitStatus(exitStatus);
    }

}
