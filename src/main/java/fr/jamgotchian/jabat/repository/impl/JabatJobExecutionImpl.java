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

import fr.jamgotchian.jabat.repository.JabatJobExecution;
import fr.jamgotchian.jabat.repository.Status;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatJobExecutionImpl implements JabatJobExecution {

    private final long id;

    private volatile Status status = Status.STARTING;

    private String exitStatus;

    private final Date createTime;

    private final List<Long> stepExecutionIds = new ArrayList<Long>();

    public JabatJobExecutionImpl(long id) {
        this.id = id;
        createTime = new Date();
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
    public void setStatus(Status status) {
        this.status = status;
    }

    @Override
    public Date getStartTime() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Date getEndTime() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getExitStatus() {
        return exitStatus;
    }

    @Override
    public Date getCreateTime() {
        return createTime;
    }

    @Override
    public Date getLastUpdatedTime() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Properties getJobParameters() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public List<Long> getStepExecutionIds() {
        return stepExecutionIds;
    }

}
