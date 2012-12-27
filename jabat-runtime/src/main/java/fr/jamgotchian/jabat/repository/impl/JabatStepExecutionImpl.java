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
package fr.jamgotchian.jabat.repository.impl;

import fr.jamgotchian.jabat.repository.JabatStepExecution;
import fr.jamgotchian.jabat.repository.BatchStatus;
import java.util.Date;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatStepExecutionImpl implements JabatStepExecution {

    private final long id;

    private volatile BatchStatus status = BatchStatus.STARTING;

    private final Date startTime;

    private Date endTime;

    private String exitStatus;

    private Object userPersistentData;

    private MetricImpl[] metrics;

    public JabatStepExecutionImpl(long id) {
        this.id = id;
        this.startTime = new Date();
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getStatus() {
        return status.name();
    }

    @Override
    public BatchStatus getStatusEnum() {
        return status;
    }

    @Override
    public void setStatus(BatchStatus status) {
        this.status = status;
    }

    @Override
    public Date getStartTime() {
        return startTime;
    }

    @Override
    public Date getEndTime() {
        return endTime;
    }

    @Override
    public void setEndTime(Date endTime) {
        this.endTime = endTime;
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
    public Object getUserPersistentData() {
        return userPersistentData;
    }

    @Override
    public void setUserPersistentData(Object userPersistentData) {
        this.userPersistentData = userPersistentData;
    }

    @Override
    public MetricImpl[] getMetrics() {
        return metrics;
    }

    @Override
    public void setMetrics(MetricImpl[] metrics) {
        this.metrics = metrics;
    }

}
