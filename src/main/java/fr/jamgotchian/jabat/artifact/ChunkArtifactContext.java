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

import fr.jamgotchian.jabat.job.ArtifactRef;
import javax.batch.spi.ArtifactFactory;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ChunkArtifactContext extends ArtifactContext {

    public ChunkArtifactContext(ArtifactFactory factory) {
        super(factory);
    }

    public ReadItemArtifact createItemReader(ArtifactRef ref) throws Exception {
        Object obj = factory.create(ref.getName());
        ReadItemArtifact reader = new ReadItemArtifact(obj, ref);
        addArtifact(reader);
        return reader;
    }

    public ProcessItemArtifact createItemProcessor(ArtifactRef ref, Class<?> itemType) throws Exception {
        Object obj = factory.create(ref.getName());
        ProcessItemArtifact processor = new ProcessItemArtifact(obj, ref, itemType);
        addArtifact(processor);
        return processor;
    }

    public WriteItemsArtifact createItemWriter(ArtifactRef ref, Class<?> outputItemType) throws Exception {
        Object obj = factory.create(ref.getName());
        WriteItemsArtifact writer = new WriteItemsArtifact(obj, ref, outputItemType);
        addArtifact(writer);
        return writer;
    }

}
