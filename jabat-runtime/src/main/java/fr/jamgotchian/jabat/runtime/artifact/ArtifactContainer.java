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

import com.google.common.collect.Iterables;
import fr.jamgotchian.jabat.jobxml.model.ChunkStep;
import fr.jamgotchian.jabat.jobxml.model.Step;
import fr.jamgotchian.jabat.runtime.checkpoint.ItemCheckpointAlgorithm;
import fr.jamgotchian.jabat.runtime.checkpoint.TimeCheckpointAlgorithm;
import fr.jamgotchian.jabat.runtime.context.ThreadContext;
import fr.jamgotchian.jabat.runtime.util.JabatRuntimeException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.batch.api.Batchlet;
import javax.batch.api.CheckpointAlgorithm;
import javax.batch.api.PartitionAnalyzer;
import javax.batch.api.PartitionCollector;
import javax.batch.api.PartitionMapper;
import javax.batch.api.PartitionReducer;
import javax.batch.api.parameters.PartitionPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A container for managing artifacts lifecycle.
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ArtifactContainer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ArtifactContainer.class);

    private final BatchXml batchXml;

    private final ArtifactFactory factory;

    private final Set<Batchlet> runningBatchlets;

    private class MonitoredBatchlet implements Batchlet {

        private final Batchlet batchlet;

        public MonitoredBatchlet(Batchlet batchlet) {
            this.batchlet = batchlet;
        }

        @Override
        public String process() throws Exception {
            try {
                runningBatchlets.add(batchlet);
                return batchlet.process();
            } finally {
                runningBatchlets.remove(batchlet);
            }
        }

        @Override
        public void stop() throws Exception {
            batchlet.process();
        }

    }

    private final List<Object> managedObjects = new ArrayList<Object>();

    private final List<Object> objects = new ArrayList<Object>();

    public ArtifactContainer(BatchXml batchXml, ArtifactFactory factory,
                             Set<Batchlet> runningBatchlets) {
        this.batchXml = batchXml;
        this.factory = factory;
        this.runningBatchlets = runningBatchlets;
    }

    private Object createFromBatchXml(String name, Class<?> type) {
        String className = batchXml.getArtifactClass(name);
        Object obj = null;
        if (className != null) {
            Class<?> clazz;
            try {
                clazz = Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new JabatRuntimeException("Batch artifact class '"
                        + className + "' not found");
            }
            if (!type.isAssignableFrom(clazz)) {
                throw new JabatRuntimeException("Expected artifact type is "
                        + type.getName() + ", instead of " + clazz.getName());
            }
            try {
                obj = clazz.newInstance();
                objects.add(obj);
                ThreadContext.getInstance().inject(obj, name);
            } catch (ReflectiveOperationException e) {
                throw new JabatRuntimeException(e);
            }
        }
        return obj;
    }

    private Object createFromDiFramework(String name, Class<?> type) {
        Object obj = factory.create(name);
        if (obj != null) {
            managedObjects.add(obj);
            if (!type.isAssignableFrom(obj.getClass())) {
                throw new JabatRuntimeException("Expected artifact type is "
                        + type.getName() + ", instead of " + obj.getClass().getName());
            }
        }
        return obj;
    }

    public <T> T create(String name, Class<T> type) {
        if (!ArtifactType.isArtifactType(type)) {
            throw new JabatRuntimeException(type.getName()
                    + " is not a batch artifact type");
        }
        Object obj;
        if (factory != null) {
            obj = createFromDiFramework(name, type);
            if (obj == null) {
                obj = createFromBatchXml(name, type);
            }
        } else {
            obj = createFromBatchXml(name, type);
        }
        if (obj == null) {
            throw new JabatRuntimeException("Batch artifact '" + name
                    + "' not found");
        }
        if (obj instanceof Batchlet) {
            return (T) new MonitoredBatchlet((Batchlet) obj);
        } else {
            return (T) obj;
        }
    }

    public PartitionPlan createPartitionPlan(Step step) throws Exception {
        if (step.getPartitionMapper() != null) {
            // dynamic defintion of the partition plan though an artifact
            String ref = step.getPartitionMapper().getRef();
            PartitionMapper mapper = create(ref, PartitionMapper.class);
            return mapper.mapPartitions();
        } else {
            // static defintion of the partition plan
            return step.getPartitionPlan();
        }
    }

    public PartitionReducer createPartitionReducer(Step step) {
        if (step.getPartitionReducer() != null) {
            String ref = step.getPartitionReducer().getRef();
            return create(ref, PartitionReducer.class);
        }
        return null;
    }

    public PartitionCollector createPartitionCollector(Step step) {
        if (step.getPartitionCollector() != null) {
            String ref= step.getPartitionCollector().getRef();
            return create(ref, PartitionCollector.class);
        }
        return null;
    }

    public PartitionAnalyzer createPartitionAnalyser(Step step) {
        if (step.getPartitionAnalyzer() != null) {
            String ref = step.getPartitionAnalyzer().getRef();
            return create(ref, PartitionAnalyzer.class);
        }
        return null;
    }

    public CheckpointAlgorithm createCheckpointAlgorithm(ChunkStep step) {
        switch (step.getCheckpointPolicy()) {
            case ITEM:
                return new ItemCheckpointAlgorithm(step.getCommitInterval());
            case TIME:
                return new TimeCheckpointAlgorithm(step.getCommitInterval());
            case CUSTOM:
                {
                    String ref = step.getCheckpointAlgo().getRef();
                    return create(ref, CheckpointAlgorithm.class);
                }
            default:
                throw new InternalError();
        }
    }

    public <T> Iterable<T> get(Class<T> type) {
        List<T> result = new ArrayList<T>();
        for (Object obj : Iterables.concat(objects, managedObjects)) {
            if (type.isAssignableFrom(obj.getClass())) {
                result.add((T) obj);
            }
        }
        return result;
    }

    public void release() {
        for (Object obj : managedObjects) {
            try {
                factory.destroy(obj);
            } catch (Throwable t) {
                LOGGER.error(t.toString(), t);
            }
        }
        objects.clear();
        managedObjects.clear();
    }

}
