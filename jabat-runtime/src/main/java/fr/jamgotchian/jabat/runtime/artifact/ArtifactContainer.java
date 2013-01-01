/*
 * Copyright 2013 Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>.
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
package fr.jamgotchian.jabat.runtime.artifact;

import fr.jamgotchian.jabat.runtime.artifact.annotation.BatchletProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.CheckpointAlgorithmProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.ItemProcessorProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.ItemReaderProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.ItemWriterProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.JobListenerProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.PartitionAnalyzerProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.PartitionCollectorProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.PartitionMapperProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.PartitionReducerProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.StepListenerProxy;
import fr.jamgotchian.jabat.runtime.util.JabatRuntimeException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.batch.api.Batchlet;
import javax.batch.api.CheckpointAlgorithm;
import javax.batch.api.ItemProcessor;
import javax.batch.api.ItemReader;
import javax.batch.api.ItemWriter;
import javax.batch.api.JobListener;
import javax.batch.api.PartitionAnalyzer;
import javax.batch.api.PartitionCollector;
import javax.batch.api.PartitionMapper;
import javax.batch.api.PartitionReducer;
import javax.batch.api.StepListener;

/**
 * A container for managing artifacts lifecycle.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ArtifactContainer {

    private static final Map<Class<?>, Class<?>> ARTIFACT_TYPES;

    static {
        Map<Class<?>, Class<?>> tmp = new HashMap<Class<?>, Class<?>>();
        tmp.put(Batchlet.class, BatchletProxy.class);
        tmp.put(CheckpointAlgorithm.class, CheckpointAlgorithmProxy.class);
        tmp.put(ItemProcessor.class, ItemProcessorProxy.class);
        tmp.put(ItemReader.class, ItemReaderProxy.class);
        tmp.put(ItemWriter.class, ItemWriterProxy.class);
        tmp.put(JobListener.class, JobListenerProxy.class);
        tmp.put(PartitionAnalyzer.class, PartitionAnalyzerProxy.class);
        tmp.put(PartitionCollector.class, PartitionCollectorProxy.class);
        tmp.put(PartitionMapper.class, PartitionMapperProxy.class);
        tmp.put(PartitionReducer.class, PartitionReducerProxy.class);
        tmp.put(StepListener.class, StepListenerProxy.class);
        ARTIFACT_TYPES = Collections.unmodifiableMap(tmp);
    }

    private final ArtifactFactory factory;

    private final List<Object> objects = new ArrayList<Object>();

    public ArtifactContainer(ArtifactFactory factory) {
        this.factory = factory;
    }

    public <T> T create(String ref, Class<T> type) throws Exception {
        if (!ARTIFACT_TYPES.containsKey(type)) {
            throw new JabatRuntimeException(type.getName()
                    + " is not a batch artifact type");
        }
        Object obj = factory.create(ref);
        objects.add(obj);
        if (type.isAssignableFrom(obj.getClass())) {
            return (T) obj;
        } else {
            // return a proxy
            return (T) ARTIFACT_TYPES.get(type).getConstructor(Object.class).newInstance(obj);
        }
    }

    public <T> Iterable<T> get(Class<T> type) {
        List<T> result = new ArrayList<T>();
        for (Object obj : objects) {
            if (type.isAssignableFrom(obj.getClass())) {
                result.add((T) obj);
            }
        }
        return result;
    }

    public void release() throws Exception {
        for (Object obj : objects) {
            factory.destroy(obj);
        }
        objects.clear();
    }

}
