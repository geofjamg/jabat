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
package fr.jamgotchian.jabat.runtime.artifact.context;

import fr.jamgotchian.jabat.runtime.ArtifactFactory;
import fr.jamgotchian.jabat.runtime.artifact.annotation.CheckpointAlgorithmProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.ItemProcessorProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.ItemReaderProxy;
import fr.jamgotchian.jabat.runtime.artifact.annotation.ItemWriterProxy;
import javax.batch.api.CheckpointAlgorithm;
import javax.batch.api.ItemProcessor;
import javax.batch.api.ItemReader;
import javax.batch.api.ItemWriter;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ChunkArtifactContext extends StepArtifactContext {

    public ChunkArtifactContext(ArtifactFactory factory) {
        super(factory);
    }

    public ItemReader createItemReader(String ref) throws Exception {
        Object obj = create(ref);
        if (obj instanceof ItemReader) {
            return (ItemReader) obj;
        } else {
            return new ItemReaderProxy(obj);
        }
    }

    public ItemProcessor createItemProcessor(String ref) throws Exception {
        Object obj = create(ref);
        if (obj instanceof ItemProcessor) {
            return (ItemProcessor) obj;
        } else {
            return new ItemProcessorProxy(obj);
        }
    }

    public ItemWriter createItemWriter(String ref) throws Exception {
        Object obj = create(ref);
        if (obj instanceof ItemWriter) {
            return (ItemWriter) obj;
        } else {
            return new ItemWriterProxy(obj);
        }
    }

    public CheckpointAlgorithm createCheckpointAlgorithm(String ref) throws Exception {
        Object obj = create(ref);
        if (obj instanceof CheckpointAlgorithm) {
            return (CheckpointAlgorithm) obj;
        } else {
            return new CheckpointAlgorithmProxy(obj);
        }
    }

}
