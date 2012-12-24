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
package fr.jamgotchian.jabat.jobxml.model;

import fr.jamgotchian.jabat.util.JabatException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import javax.batch.api.parameters.PartitionPlan;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class PartitionPlanBuilder {

    private int partitionCount = 1;

    private Integer threadCount;

    private final Map<Integer, Properties> propertiesPerPartition
            = new HashMap<Integer, Properties>();

    public PartitionPlanBuilder setPartitionCount(int partitionCount) {
        this.partitionCount = partitionCount;
        return this;
    }

    public PartitionPlanBuilder setThreadCount(int threadCount) {
        this.threadCount = threadCount;
        return this;
    }

    public PartitionPlanBuilder addProperties(int partition, Properties properties) {
        if (partition < 0 || partition >= partitionCount) {
            throw new JabatException("Wrong partition number " + partition);
        }
        if (!this.propertiesPerPartition.containsKey(partition)) {
            this.propertiesPerPartition.put(partition, new Properties());
        }
        this.propertiesPerPartition.get(partition).putAll(properties);
        return this;
    }

    private int getThreadCount() {
        return threadCount == null ? partitionCount : threadCount;
    }

    public PartitionPlan build() {
        if (partitionCount <= 0) {
            throw new JabatException("Incorrect partition count " + partitionCount);
        }
        if (threadCount != null && threadCount <= 0) {
            throw new JabatException("Incorrect thread count " + threadCount);
        }
        Properties[] properties = new Properties[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            if (propertiesPerPartition.containsKey(i)) {
                properties[i] = propertiesPerPartition.get(i);
            } else {
                properties[i] = new Properties();
            }
        }
        return new PartitionPlanImpl(partitionCount, getThreadCount(), properties);
    }
}
