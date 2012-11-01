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
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class BatchletStepNode extends StepNode {

    private final ArtifactRef batchletRef;

    public BatchletStepNode(String id, NodeContainer container, String next,
                            Properties properties, ArtifactRef batchletRef) {
        super(id, container, next, properties);
        this.batchletRef = batchletRef;
    }

    public ArtifactRef getBatchletRef() {
        return batchletRef;
    }

    @Override
    public ArtifactRef getRef(String ref) {
        if (this.batchletRef.getName().equals(ref)) {
            return this.batchletRef;
        } else {
            throw new JabatException("Artifact " + ref + " not found");
        }
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }

}
