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

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class Split extends AbstractNodeContainer implements Chainable {

    private final String next;

    private Artifact collector;

    private Artifact analyser;

    Split(String id, String next, Properties properties) {
        super(id, properties);
        this.next = next;
    }

    public Split addFlow(Flow flow) {
        addNode(flow);
        return this;
    }

    @Override
    public String getNext() {
        return next;
    }

    public Artifact getCollector() {
        return collector;
    }

    public void setCollector(Artifact collector) {
        this.collector = collector;
    }

    public Artifact getAnalyser() {
        return analyser;
    }

    public void setAnalyser(Artifact analyser) {
        this.analyser = analyser;
    }

    @Override
    public Artifact getArtifact(String ref) {
        if (collector != null && collector.getRef().equals(ref)) {
            return collector;
        } else if (analyser != null && analyser.getRef().equals(ref)) {
            return analyser;
        } else {
            throw new JabatException("Artifact " + ref + " not found");
        }
    }

    @Override
    public List<Artifact> getArtifacts() {
        List<Artifact> artifacts = new ArrayList<Artifact>();
        if (collector != null) {
            artifacts.add(collector);
        }
        if (analyser != null) {
            artifacts.add(analyser);
        }
        return artifacts;
    }

    @Override
    public <A> void accept(NodeVisitor<A> visitor, A arg) {
        visitor.visit(this, arg);
    }

}
