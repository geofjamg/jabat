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

import fr.jamgotchian.jabat.repository.JabatJobInstance;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class JabatJobInstanceImpl implements JabatJobInstance {

    private final String jobId;

    private final long id;

    private final List<Long> executionIds = new ArrayList<Long>();

    public JabatJobInstanceImpl(String jobId, long id) {
        this.jobId = jobId;
        this.id = id;
    }

    @Override
    public String getJobName() {
        return jobId;
    }

    @Override
    public long getInstanceId() {
        return id;
    }

    @Override
    public List<Long> getExecutionIds() {
        return executionIds;
    }

    @Override
    public long getLastExecutionId() {
        return executionIds.get(executionIds.size()-1);
    }
}
