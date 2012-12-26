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
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ChunkStepBuilder extends StepBuilder<ChunkStepBuilder, ChunkStep> {

    private CheckpointPolicy checkpointPolicy;

    private int commitInterval = 10;

    private Artifact checkpointAlgo;

    private Integer bufferSize;

    private int retryLimit = -1;

    private int skipLimit = -1;

    private Artifact reader;

    private Artifact processor;

    private Artifact writer;

    private final Set<Class<?>> includedSkippableExceptionClasses = new HashSet<Class<?>>();

    private final Set<Class<?>> excludedSkippableExceptionClasses = new HashSet<Class<?>>();

    private final Set<Class<?>> includedRetryableExceptionClasses = new HashSet<Class<?>>();

    private final Set<Class<?>> excludedRetryableExceptionClasses = new HashSet<Class<?>>();

    private final Set<Class<?>> includedNoRollbackExceptionClasses = new HashSet<Class<?>>();

    private final Set<Class<?>> excludedNoRollbackExceptionClasses = new HashSet<Class<?>>();

    public ChunkStepBuilder() {
    }

    @Override
    protected ChunkStepBuilder getBuilder() {
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

    public ChunkStepBuilder setCheckpointAlgo(Artifact checkpointAlgo) {
        this.checkpointAlgo = checkpointAlgo;
        return this;
    }

    public ChunkStepBuilder setBufferSize(int bufferSize) {
        if (bufferSize < 1) {
            throw new JabatException("Chunk buffer size is expected to be greater than 0");
        }
        this.bufferSize = bufferSize;
        return this;
    }

    public ChunkStepBuilder setRetryLimit(int retryLimit) {
        if (retryLimit < 0) {
            throw new JabatException("Chunk retry limit is expected to be greater or equal than 0");
        }
        this.retryLimit = retryLimit;
        return this;
    }

    public ChunkStepBuilder setSkipLimit(int skipLimit) {
        if (skipLimit < 0) {
            throw new JabatException("Chunk skip limit is expected to be greater or equal than 0");
        }
        this.skipLimit = skipLimit;
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

    public ChunkStepBuilder includeSkippableException(Class<?> clazz) {
        this.includedSkippableExceptionClasses.add(clazz);
        return this;
    }

    public ChunkStepBuilder excludeSkippableException(Class<?> clazz) {
        this.excludedSkippableExceptionClasses.add(clazz);
        return this;
    }

    public ChunkStepBuilder includeRetryableException(Class<?> clazz) {
        this.includedRetryableExceptionClasses.add(clazz);
        return this;
    }

    public ChunkStepBuilder excludeRetryableException(Class<?> clazz) {
        this.excludedRetryableExceptionClasses.add(clazz);
        return this;
    }

    public ChunkStepBuilder includeNoRollbackException(Class<?> clazz) {
        this.includedNoRollbackExceptionClasses.add(clazz);
        return this;
    }

    public ChunkStepBuilder excludeNoRollbackException(Class<?> clazz) {
        this.excludedNoRollbackExceptionClasses.add(clazz);
        return this;
    }

    @Override
    public ChunkStep build() {
        check();
        if (reader == null) {
            throw new JabatException("Chunk reader artifact is not set");
        }
        if (processor == null) {
            throw new JabatException("Chunk processor artifact is not set");
        }
        if (writer == null) {
            throw new JabatException("Chunk writer artifact is not set");
        }
        CheckpointPolicy policy = getCheckpointPolicy();
        if (policy == CheckpointPolicy.CUSTOM && checkpointAlgo == null) {
            throw new JabatException("A checkpoint algorithm artifact is expected for a custom checkpoint policy");
        }
        ExceptionClassFilter skippableExceptionClasses =
                new ExceptionClassFilter(includedSkippableExceptionClasses,
                                         excludedSkippableExceptionClasses);
        ExceptionClassFilter retryableExceptionClasses
                = new ExceptionClassFilter(includedRetryableExceptionClasses,
                                           excludedRetryableExceptionClasses);
        ExceptionClassFilter noRollbackExceptionClasses
                = new ExceptionClassFilter(includedNoRollbackExceptionClasses,
                                           excludedNoRollbackExceptionClasses);
        ChunkStep chunk = new ChunkStep(id, next, startLimit, allowStartIfComplete,
                                        properties, partitionPlan, partitionMapper,
                                        partitionReducer, partitionCollector, partitionAnalyser,
                                        listeners, controlElements,
                                        reader, processor, writer,
                                        policy, commitInterval,
                                        checkpointAlgo, getBufferSize(), retryLimit, skipLimit,
                                        skippableExceptionClasses, retryableExceptionClasses,
                                        noRollbackExceptionClasses);
        return chunk;
    }
}
