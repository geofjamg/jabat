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
package fr.jamgotchian.jabat.jobxml.model;

import fr.jamgotchian.jabat.util.JabatException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.batch.api.parameters.PartitionPlan;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ChunkStepBuilder {

    private String id;

    private String next;

    private Properties properties = new Properties();

    private List<Artifact> listeners = new ArrayList<Artifact>();

    private PartitionPlan partitionPlan;

    private Artifact partitionMapper;

    private Artifact partitionReducer;

    private Artifact partitionCollector;

    private Artifact partitionAnalyser;

    private CheckpointPolicy checkpointPolicy;

    private int commitInterval = 10;

    private Integer bufferSize;

    private int retryLimit = -1;

    private Artifact reader;

    private Artifact processor;

    private Artifact writer;

    public ChunkStepBuilder() {
    }

    public ChunkStepBuilder setId(String id) {
        this.id = id;
        return this;
    }

    public ChunkStepBuilder setNext(String next) {
        this.next = next;
        return this;
    }

    public ChunkStepBuilder setProperty(String name, String value) {
        properties.setProperty(name, value);
        return this;
    }

    public ChunkStepBuilder setProperties(Properties properties) {
        if (properties == null) {
            this.properties = new Properties();
        } else {
            this.properties = properties;
        }
        return this;
    }

    public ChunkStepBuilder addListener(Artifact listener) {
        listeners.add(listener);
        return this;
    }

    public ChunkStepBuilder setListeners(List<Artifact> listeners) {
        if (listeners == null) {
            this.listeners = new ArrayList<Artifact>();
        } else {
            this.listeners = listeners;
        }
        return this;
    }

    public ChunkStepBuilder setPartitionPlan(PartitionPlan partitionPlan) {
        this.partitionPlan = partitionPlan;
        return this;
    }

    public ChunkStepBuilder setPartitionMapper(Artifact partitionMapper) {
        this.partitionMapper = partitionMapper;
        return this;
    }

    public ChunkStepBuilder setPartitionReducer(Artifact partitionReducer) {
        this.partitionReducer = partitionReducer;
        return this;
    }

    public ChunkStepBuilder setPartitionCollector(Artifact partitionCollector) {
        this.partitionCollector = partitionCollector;
        return this;
    }

    public ChunkStepBuilder setPartitionAnalyser(Artifact partitionAnalyser) {
        this.partitionAnalyser = partitionAnalyser;
        return this;
    }

    public ChunkStepBuilder setReader(Artifact reader) {
        this.reader = reader;
        return this;
    }

    public ChunkStepBuilder setProcessor(Artifact processor) {
        this.processor = processor;
        return this;
    }

    public ChunkStepBuilder setWriter(Artifact writer) {
        this.writer = writer;
        return this;
    }

    public ChunkStepBuilder setCheckpointPolicy(CheckpointPolicy checkpointPolicy) {
        this.checkpointPolicy = checkpointPolicy;
        return this;
    }

    public ChunkStepBuilder setCommitInterval(int commitInterval) {
        if (commitInterval < 1) {
            throw new JabatException("Chunk commit interval should be greater than 0");
        }
        this.commitInterval = commitInterval;
        return this;
    }

    public ChunkStepBuilder setBufferSize(int bufferSize) {
        if (bufferSize < 1) {
            throw new JabatException("Chunk buffer size should be greater than 0");
        }
        this.bufferSize = bufferSize;
        return this;
    }

    public ChunkStepBuilder setRetryLimit(int retryLimit) {
        if (retryLimit < 0) {
            throw new JabatException("Chunk retry limit should be greater or equal than 0");
        }
        this.retryLimit = retryLimit;
        return this;
    }

    private CheckpointPolicy getCheckpointPolicy() {
        return checkpointPolicy == null ? CheckpointPolicy.ITEM : checkpointPolicy;
    }

    private int getBufferSize() {
        if (bufferSize != null) {
            return bufferSize;
        } else {
            switch (getCheckpointPolicy()) {
                case ITEM:
                    return commitInterval;
                case TIME:
                case CUSTOM:
                    return 10;
                default:
                    throw new InternalError();
            }
        }
    }

    public ChunkStep build() {
        if (id == null) {
            throw new JabatException("Chunk id is not set");
        }
        if (reader == null) {
            throw new JabatException("Chunk reader artifact is not set");
        }
        if (processor == null) {
            throw new JabatException("Chunk processor artifact is not set");
        }
        if (writer == null) {
            throw new JabatException("Chunk writer artifact is not set");
        }
        ChunkStep chunk = new ChunkStep(id, next, properties, partitionPlan, partitionMapper,
                                        partitionReducer, partitionCollector, partitionAnalyser,
                                        listeners, reader, processor, writer,
                                        getCheckpointPolicy(), commitInterval,
                                        getBufferSize(), retryLimit);
        return chunk;
    }
}
