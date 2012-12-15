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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class Flow extends AbstractNodeContainer implements Chainable {

    private final String next;

    Flow(String id, String next, Properties properties) {
        super(id, properties);
        this.next = next;
    }

    public Flow addBatchlet(BatchletStep step) {
        addNode(step);
        return this;
    }

    public Flow addChunk(ChunkStep step) {
        addNode(step);
        return this;
    }

    public Flow addFlow(Flow flow) {
        addNode(flow);
        return this;
    }

    public Flow addSplit(Split split) {
        addNode(split);
        return this;
    }

    public Flow addDecision(Decision decision) {
        addNode(decision);
        return this;
    }

    @Override
    public String getNext() {
        return next;
    }

    @Override
    public Artifact getArtifact(String ref) {
        throw new JabatException("Artifact " + ref + " not found");
    }

    @Override
    public List<Artifact> getArtifacts() {
        return Collections.emptyList();
    }

    @Override
    public <A> void accept(NodeVisitor<A> visitor, A arg) {
        visitor.visit(this, arg);
    }

}
