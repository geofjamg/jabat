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

import fr.jamgotchian.jabat.artifact.BatchletArtifact;
import java.util.Date;
import javax.batch.runtime.StepExecution;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatStepExecution implements StepExecution {

    private final long id;

    private volatile Status status = Status.STARTING;

    private final Date startTime;

    private Date endTime;

    private String exitStatus;

    private Object userPersistentData;

    private MetricImpl[] metrics;

    private volatile BatchletArtifact batchletArtifact;

    public JabatStepExecution(long id) {
        this.id = id;
        this.startTime = new Date();
    }

    public long getId() {
        return id;
    }

    @Override
    public String getStatus() {
        return status.name();
    }

    public void setStatus(Status status) {
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

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    @Override
    public String getExitStatus() {
        return exitStatus;
    }

    public void setExitStatus(String exitStatus) {
        this.exitStatus = exitStatus;
    }

    @Override
    public Object getUserPersistentData() {
        return userPersistentData;
    }

    public void setUserPersistentData(Object userPersistentData) {
        this.userPersistentData = userPersistentData;
    }

    @Override
    public MetricImpl[] getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricImpl[] metrics) {
        this.metrics = metrics;
    }

    public BatchletArtifact getBatchletArtifact() {
        return batchletArtifact;
    }

    public void setBatchletArtifact(BatchletArtifact batchletArtifact) {
        this.batchletArtifact = batchletArtifact;
    }

}
