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
package fr.jamgotchian.jabat.job;

import fr.jamgotchian.jabat.util.JabatException;
import fr.jamgotchian.jabat.util.Setter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ChunkStepBuilder {

    private final NodeContainer container;

    private String id;

    private String next;

    private final Properties properties = new Properties();

    private final List<Artifact> listeners = new ArrayList<Artifact>();

    private CheckpointPolicy checkpointPolicy;

    private int commitInterval = 10;

    private Integer bufferSize;

    private int retryLimit = -1;

    private Artifact reader;

    private Artifact processor;

    private Artifact writer;

    public ChunkStepBuilder(NodeContainer container) {
        this.container = container;
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

    public ArtifactBuilder<ChunkStepBuilder> newReader() {
        return new ArtifactBuilder<ChunkStepBuilder>(this, new Setter<Artifact>() {
            @Override
            public void set(Artifact artifact) {
                ChunkStepBuilder.this.reader = artifact;
            }
        });
    }

    public ArtifactBuilder<ChunkStepBuilder> newProcessor() {
        return new ArtifactBuilder<ChunkStepBuilder>(this, new Setter<Artifact>() {
            @Override
            public void set(Artifact artifact) {
                ChunkStepBuilder.this.processor = artifact;
            }
        });
    }

    public ArtifactBuilder<ChunkStepBuilder> newWriter() {
        return new ArtifactBuilder<ChunkStepBuilder>(this, new Setter<Artifact>() {
            @Override
            public void set(Artifact artifact) {
                ChunkStepBuilder.this.writer = artifact;
            }
        });
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
        ChunkStep chunk = new ChunkStep(id, container, next, properties, listeners,
                                        reader, processor, writer,
                                        getCheckpointPolicy(), commitInterval,
                                        getBufferSize(), retryLimit);
        container.addNode(chunk);
        return chunk;
    }
}
