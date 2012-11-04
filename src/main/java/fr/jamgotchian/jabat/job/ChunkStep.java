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
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class ChunkStep extends Step {

    private final Artifact readerArtifact;

    private final Artifact processorArtifact;

    private final Artifact writerArtifact;

    private final CheckpointPolicy checkpointPolicy;

    private final int commitInterval;

    private Artifact checkpointAlgoArtifact;
    
    private final int bufferSize;

    private final int retryLimit;

    ChunkStep(String id, NodeContainer container, String next, 
              Properties properties, List<Artifact> listenerArtifacts,
              Artifact readerArtifact, Artifact processorArtifact, Artifact writerArtifact,
              CheckpointPolicy checkpointPolicy, int commitInterval,
              int bufferSize, int retryLimit) {
        super(id, container, next, properties, listenerArtifacts);
        this.readerArtifact = readerArtifact;
        this.processorArtifact = processorArtifact;
        this.writerArtifact = writerArtifact;
        this.checkpointPolicy = checkpointPolicy;
        this.commitInterval = commitInterval;
        this.bufferSize = bufferSize;
        this.retryLimit = retryLimit;
    }

    public Artifact getReaderArtifact() {
        return readerArtifact;
    }

    public Artifact getProcessorArtifact() {
        return processorArtifact;
    }

    public Artifact getWriterArtifact() {
        return writerArtifact;
    }

    public CheckpointPolicy getCheckpointPolicy() {
        return checkpointPolicy;
    }

    public int getCommitInterval() {
        return commitInterval;
    }

    public Artifact getCheckpointAlgoArtifact() {
        return checkpointAlgoArtifact;
    }

    public void setCheckpointAlgoArtifact(Artifact checkpointAlgoArtifact) {
        this.checkpointAlgoArtifact = checkpointAlgoArtifact;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    @Override
    public Artifact getArtifact(String ref) {
        if (readerArtifact.getRef().equals(ref)) {
            return readerArtifact;
        } else if (processorArtifact.getRef().equals(ref)) {
            return processorArtifact;
        } else if (writerArtifact.getRef().equals(ref)) {
            return writerArtifact;
        } else if (checkpointAlgoArtifact != null 
                && checkpointAlgoArtifact.getRef().equals(ref)) {
            return checkpointAlgoArtifact;
        } else {
            throw new JabatException("Artifact " + ref + " not found");
        }
    }

    @Override
    public <A> void accept(NodeVisitor<A> visitor, A arg) {
        visitor.visit(this, arg);
    }

}