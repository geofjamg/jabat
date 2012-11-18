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
package fr.jamgotchian.jabat.artifact;

import fr.jamgotchian.jabat.artifact.annotated.PartitionAnalyserProxy;
import fr.jamgotchian.jabat.artifact.annotated.PartitionCollectorProxy;
import fr.jamgotchian.jabat.artifact.annotated.PartitionMapperProxy;
import fr.jamgotchian.jabat.artifact.annotated.PartitionReducerProxy;
import fr.jamgotchian.jabat.artifact.annotated.StepListenerProxy;
import fr.jamgotchian.jabat.spi.ArtifactFactory;
import java.util.ArrayList;
import java.util.List;
import javax.batch.api.PartitionAnalyzer;
import javax.batch.api.PartitionCollector;
import javax.batch.api.PartitionMapper;
import javax.batch.api.PartitionReducer;
import javax.batch.api.StepListener;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class StepArtifactContext extends ArtifactContext {

    private final List<StepListener> stepListeners = new ArrayList<StepListener>();

    public StepArtifactContext(ArtifactFactory factory) {
        super(factory);
    }

    public StepListener createStepListener(String ref) throws Exception {
        Object obj = create(ref);
        StepListener stepListener;
        if (obj instanceof StepListener) {
            stepListener = (StepListener) obj;
        } else {
            stepListener = new StepListenerProxy(obj);
        }
        stepListeners.add(stepListener);
        return stepListener;
    }

    public List<StepListener> getStepListeners() {
        return stepListeners;
    }

    public PartitionMapper createPartitionMapper(String ref) throws Exception {
        Object obj = create(ref);
        PartitionMapper mapper;
        if (obj instanceof PartitionMapper) {
            mapper = (PartitionMapper) obj;
        } else {
            mapper = new PartitionMapperProxy(obj);
        }
        return mapper;
    }

    public PartitionReducer createPartitionReducer(String ref) throws Exception {
        Object obj = create(ref);
        PartitionReducer reducer;
        if (obj instanceof PartitionReducer) {
            reducer = (PartitionReducer) obj;
        } else {
            reducer = new PartitionReducerProxy(obj);
        }
        return reducer;
    }

    public PartitionCollector createPartitionCollector(String ref) throws Exception {
        Object obj = create(ref);
        PartitionCollector collector;
        if (obj instanceof PartitionCollector) {
            collector = (PartitionCollector) obj;
        } else {
            collector = new PartitionCollectorProxy(obj);
        }
        return collector;
    }

    public PartitionAnalyzer createPartitionAnalyser(String ref) throws Exception {
        Object obj = create(ref);
        PartitionAnalyzer analyser;
        if (obj instanceof PartitionAnalyzer) {
            analyser = (PartitionAnalyzer) obj;
        } else {
            analyser = new PartitionAnalyserProxy(obj);
        }
        return analyser;
    }

    @Override
    public void release() throws Exception {
        super.release();
        stepListeners.clear();
    }

}
