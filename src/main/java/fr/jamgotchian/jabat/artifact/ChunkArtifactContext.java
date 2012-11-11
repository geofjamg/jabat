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
public class ChunkArtifactContext extends StepArtifactContext {

    public ChunkArtifactContext(ArtifactFactory factory) {
        super(factory);
    }

    public ItemReaderArtifactInstance createItemReader(String ref) throws Exception {
        Object obj = create(ref);
        return new ItemReaderArtifactInstance(obj);
    }

    public ItemProcessorArtifactInstance createItemProcessor(String ref, Class<?> itemType) throws Exception {
        Object obj = create(ref);
        return new ItemProcessorArtifactInstance(obj, itemType);
    }

    public ItemWriterArtifactInstance createItemWriter(String ref, Class<?> outputItemType) throws Exception {
        Object obj = create(ref);
        return new ItemWriterArtifactInstance(obj, outputItemType);
    }

    public CheckpointAlgorithmArtifactInstance createCheckpointAlgorithm(String ref) throws Exception {
        Object obj = create(ref);
        return new CheckpointAlgorithmArtifactInstance(obj);
    }

}
