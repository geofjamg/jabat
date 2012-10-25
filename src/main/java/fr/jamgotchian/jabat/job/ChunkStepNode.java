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

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ChunkStepNode extends StepNode {

    private final String readerRef;

    private final String processorRef;

    private final String writerRef;

    private final CheckpointPolicy checkpointPolicy;

    private final int commitInterval;

    private final int bufferSize;

    private final int retryLimit;

    public ChunkStepNode(String id, NodeContainer container, String next, String readerRef,
                         String processorRef, String writerRef, CheckpointPolicy checkpointPolicy,
                         int commitInterval, int bufferSize, int retryLimit) {
        super(id, container, next);
        this.readerRef = readerRef;
        this.processorRef = processorRef;
        this.writerRef = writerRef;
        this.checkpointPolicy = checkpointPolicy;
        this.commitInterval = commitInterval;
        this.bufferSize = bufferSize;
        this.retryLimit = retryLimit;
    }

    public String getReaderRef() {
        return readerRef;
    }

    public String getProcessorRef() {
        return processorRef;
    }

    public String getWriterRef() {
        return writerRef;
    }

    public CheckpointPolicy getCheckpointPolicy() {
        return checkpointPolicy;
    }

    public int getCommitInterval() {
        return commitInterval;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }

}
