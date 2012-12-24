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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;
import javax.batch.api.parameters.PartitionPlan;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class PartitionPlanImpl implements PartitionPlan {

    private int partitionCount;

    private int threadCount;

    private Properties[] properties;

    public PartitionPlanImpl(int partitionCount, int threadCount, Properties[] properties) {
        this.partitionCount = partitionCount;
        this.threadCount = threadCount;
        this.properties = properties;
    }

    @Override
    public void setPartitionCount(int count) {
        throw new JabatException("Immutable partition plan");
    }

    @Override
    public void setThreadCount(int count) {
        throw new JabatException("Immutable partition plan");
    }

    @Override
    public void setPartitionProperties(Properties[] props) {
        throw new JabatException("Immutable partition plan");
    }

    @Override
    public int getPartitionCount() {
        return partitionCount;
    }

    @Override
    public int getThreadCount() {
        return threadCount;
    }

    @Override
    public Properties[] getPartitionProperties() {
        return properties;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(partitionCount);
        out.writeInt(threadCount);
        out.writeObject(properties);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        partitionCount = in.readInt();
        threadCount = in.readInt();
        properties = (Properties[]) in.readObject();
    }

}
