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

import javax.batch.spi.ArtifactFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ChunkArtifactContext extends ArtifactContext {

    public ChunkArtifactContext(ArtifactFactory factory) {
        super(factory);
    }

    public ReadItemArtifactInstance createItemReader(String ref) throws Exception {
        Object obj = factory.create(ref);
        ReadItemArtifactInstance instance = new ReadItemArtifactInstance(obj, ref);
        addInstance(instance);
        return instance;
    }

    public ProcessItemArtifactInstance createItemProcessor(String ref, Class<?> itemType) throws Exception {
        Object obj = factory.create(ref);
        ProcessItemArtifactInstance instance = new ProcessItemArtifactInstance(obj, ref, itemType);
        addInstance(instance);
        return instance;
    }

    public WriteItemsArtifactInstance createItemWriter(String ref, Class<?> outputItemType) throws Exception {
        Object obj = factory.create(ref);
        WriteItemsArtifactInstance instance = new WriteItemsArtifactInstance(obj, ref, outputItemType);
        addInstance(instance);
        return instance;
    }

    public CheckpointAlgorithmArtifactInstance createCheckpointAlgorithm(String ref) throws Exception {
        Object obj = factory.create(ref);
        CheckpointAlgorithmArtifactInstance instance = new CheckpointAlgorithmArtifactInstance(obj, ref);
        addInstance(instance);
        return instance;
    }

}
