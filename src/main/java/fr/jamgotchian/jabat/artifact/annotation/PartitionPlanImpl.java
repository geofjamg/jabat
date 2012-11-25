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
package fr.jamgotchian.jabat.artifact.annotation;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;
import javax.batch.api.parameters.PartitionPlan;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class PartitionPlanImpl implements PartitionPlan{

    private int partitionCount;

    private int threadCount;

    private Properties[] properties;

    @Override
    public void setPartitionCount(int count) {
        partitionCount = count;
    }

    @Override
    public void setThreadCount(int count) {
        threadCount = count;
    }

    @Override
    public void setPartitionProperties(Properties[] props) {
        properties = props;
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
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
