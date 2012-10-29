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
public class ChunkArtifactContext {

    private final ArtifactFactory factory;

    private ReadItemArtifact reader;

    private ProcessItemArtifact processor;

    private WriteItemsArtifact writer;

    public ChunkArtifactContext(ArtifactFactory factory) {
        this.factory = factory;
    }

    public void create(String readerRef, String processorRef, String writerRef) throws Exception {
        Object readerObj = factory.create(readerRef);
        reader = new ReadItemArtifact(readerObj);
        Class<?> itemType = reader.getItemType();
        Object processorObj = factory.create(processorRef);
        processor = new ProcessItemArtifact(processorObj, itemType);
        Class<?> outputItemType = processor.getOutputItemType();
        Object writerObj = factory.create(writerRef);
        writer = new WriteItemsArtifact(writerObj, outputItemType);
    }

    public ReadItemArtifact getReader() {
        return reader;
    }

    public ProcessItemArtifact getProcessor() {
        return processor;
    }

    public WriteItemsArtifact getWriter() {
        return writer;
    }

    public void release() throws Exception {
        if (reader != null) {
            factory.destroy(reader.getObject());
        }
        if (processor != null) {
            factory.destroy(processor.getObject());
        }
        if (writer != null) {
            factory.destroy(writer.getObject());
        }
    }
}
