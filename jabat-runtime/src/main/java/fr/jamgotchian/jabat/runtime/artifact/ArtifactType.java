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

import com.google.common.collect.Sets;
import java.util.Collections;
import java.util.Set;
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
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ArtifactType {

    private static final Set<Class<?>> TYPES
            = Collections.unmodifiableSet(Sets.newHashSet(Batchlet.class,
                                                          CheckpointAlgorithm.class,
                                                          ItemProcessor.class,
                                                          ItemReader.class,
                                                          ItemWriter.class,
                                                          JobListener.class,
                                                          PartitionAnalyzer.class,
                                                          PartitionCollector.class,
                                                          PartitionMapper.class,
                                                          PartitionReducer.class,
                                                          StepListener.class));

    private ArtifactType() {
    }

    public static boolean isArtifactType(Class<?> type) {
        return TYPES.contains(type);
    }

}
