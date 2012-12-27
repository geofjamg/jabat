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

import fr.jamgotchian.jabat.jobxml.util.JobXmlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import javax.batch.api.parameters.PartitionPlan;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchletStep extends Step {

    private final Artifact artifact;

    BatchletStep(String id, String next, int startLimit, boolean allowStartIfComplete,
             Properties properties, PartitionPlan partitionPlan, Artifact partitionMapper,
             Artifact partitionReducer, Artifact partitionCollector, Artifact partitionAnalyser,
             List<Artifact> listeners, List<ControlElement> controlElements,
             Artifact artifact) {
        super(id, next, startLimit, allowStartIfComplete, properties, partitionPlan,
                partitionMapper, partitionReducer, partitionCollector, partitionAnalyser,
                listeners, controlElements);
        this.artifact = artifact;
    }

    @Override
    public NodeType getType() {
        return NodeType.BATCHLET_STEP;
    }

    public Artifact getArtifact() {
        return artifact;
    }

    @Override
    public Artifact getArtifact(String ref) {
        Artifact result = super.getArtifact(ref);
        if (result != null) {
            return result;
        } else if (artifact.getRef().equals(ref)) {
            return artifact;
        } else {
            throw new JobXmlException("Artifact '" + ref + "' not found");
        }
    }

    @Override
    public List<Artifact> getArtifacts() {
        List<Artifact> artifacts = new ArrayList<Artifact>(1);
        getArtifacts(artifacts);
        artifacts.add(artifact);
        return artifacts;
    }

    @Override
    public <A> void accept(NodeVisitor<A> visitor, A arg) {
        visitor.visit(this, arg);
    }

}
