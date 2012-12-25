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
public class ChunkStep extends Step {

    private final Artifact reader;

    private final Artifact processor;

    private final Artifact writer;

    private final CheckpointPolicy checkpointPolicy;

    private final int commitInterval;

    private Artifact checkpointAlgo;

    private final int bufferSize;

    private final int retryLimit;

    private final int skipLimit;

    private final ExceptionClassFilter skippableExceptionClasses;

    private final ExceptionClassFilter retryableExceptionClasses;

    private final ExceptionClassFilter noRollbackExceptionClasses;

    ChunkStep(String id, String next, int startLimit, boolean allowStartIfComplete,
            Properties properties, PartitionPlan partitionPlan, Artifact partitionMapper,
            Artifact partitionReducer, Artifact partitionCollector, Artifact partitionAnalyser,
            List<Artifact> listeners, List<TerminatingElement> terminatingElements,
            Artifact reader, Artifact processor, Artifact writer,
            CheckpointPolicy checkpointPolicy, int commitInterval,
            Artifact checkpointAlgo, int bufferSize, int retryLimit, int skipLimit,
            ExceptionClassFilter skippableExceptionClasses,
            ExceptionClassFilter retryableExceptionClasses,
            ExceptionClassFilter noRollbackExceptionClasses) {
        super(id, next, startLimit, allowStartIfComplete, properties, partitionPlan,
                partitionMapper, partitionReducer, partitionCollector, partitionAnalyser,
                listeners, terminatingElements);
        this.reader = reader;
        this.processor = processor;
        this.writer = writer;
        this.checkpointPolicy = checkpointPolicy;
        this.commitInterval = commitInterval;
        this.checkpointAlgo = checkpointAlgo;
        this.bufferSize = bufferSize;
        this.retryLimit = retryLimit;
        this.skipLimit = skipLimit;
        this.skippableExceptionClasses = skippableExceptionClasses;
        this.retryableExceptionClasses = retryableExceptionClasses;
        this.noRollbackExceptionClasses = noRollbackExceptionClasses;
    }

    @Override
    public NodeType getType() {
        return NodeType.CHUNK_STEP;
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

    public Artifact getCheckpointAlgo() {
        return checkpointAlgo;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public int getRetryLimit() {
        return retryLimit;
    }

    public int getSkipLimit() {
        return skipLimit;
    }

    public ExceptionClassFilter getSkippableExceptionClasses() {
        return skippableExceptionClasses;
    }

    public ExceptionClassFilter getRetryableExceptionClasses() {
        return retryableExceptionClasses;
    }

    public ExceptionClassFilter getNoRollbackExceptionClasses() {
        return noRollbackExceptionClasses;
    }

    @Override
    public Artifact getArtifact(String ref) {
        Artifact result = super.getArtifact(ref);
        if (result != null) {
            return result;
        } else if (reader.getRef().equals(ref)) {
            return reader;
        } else if (processor.getRef().equals(ref)) {
            return processor;
        } else if (writer.getRef().equals(ref)) {
            return writer;
        } else if (checkpointAlgo != null
                && checkpointAlgo.getRef().equals(ref)) {
            return checkpointAlgo;
        } else {
            throw new JabatException("Artifact '" + ref + "' not found");
        }
    }

    @Override
    public List<Artifact> getArtifacts() {
        List<Artifact> artifacts = new ArrayList<Artifact>(3);
        getArtifacts(artifacts);
        artifacts.add(reader);
        artifacts.add(processor);
        artifacts.add(writer);
        if (checkpointAlgo != null) {
            artifacts.add(checkpointAlgo);
        }
        return artifacts;
    }

    @Override
    public <A> void accept(NodeVisitor<A> visitor, A arg) {
        visitor.visit(this, arg);
    }

}
