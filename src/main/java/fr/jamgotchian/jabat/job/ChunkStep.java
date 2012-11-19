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

    private final Artifact reader;

    private final Artifact processor;

    private final Artifact writer;

    private final CheckpointPolicy checkpointPolicy;

    private final int commitInterval;

    private Artifact checkpointAlgoArtifact;

    private final int bufferSize;

    private final int retryLimit;

    ChunkStep(String id, NodeContainer container, String next,
              Properties properties, List<Artifact> listenerArtifacts,
              Artifact reader, Artifact processor, Artifact writer,
              CheckpointPolicy checkpointPolicy, int commitInterval,
              int bufferSize, int retryLimit) {
        super(id, container, next, properties, listenerArtifacts);
        this.reader = reader;
        this.processor = processor;
        this.writer = writer;
        this.checkpointPolicy = checkpointPolicy;
        this.commitInterval = commitInterval;
        this.bufferSize = bufferSize;
        this.retryLimit = retryLimit;
    }

    public Artifact getReader() {
        return reader;
    }

    public Artifact getProcessor() {
        return processor;
    }

    public Artifact getWriter() {
        return writer;
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
        if (reader.getRef().equals(ref)) {
            return reader;
        } else if (processor.getRef().equals(ref)) {
            return processor;
        } else if (writer.getRef().equals(ref)) {
            return writer;
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
