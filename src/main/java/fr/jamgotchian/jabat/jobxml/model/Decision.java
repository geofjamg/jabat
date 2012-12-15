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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class Decision extends AbstractNode {

    private final Artifact artifact;

    private final List<ControlElement> controlElements = new ArrayList<ControlElement>();

    Decision(String id, Properties properties, Artifact artifact) {
        super(id, properties);
        this.artifact = artifact;
    }

    public Artifact getArtifact() {
        return artifact;
    }

    @Override
    public Artifact getArtifact(String ref) {
        if (artifact.getRef().equals(ref)) {
            return artifact;
        } else {
            throw new JabatException("Artifact " + ref + " not found");
        }
    }

    @Override
    public List<Artifact> getArtifacts() {
        return Collections.singletonList(artifact);
    }

    public void addControlElement(ControlElement ctrlElt) {
        controlElements.add(ctrlElt);
    }

    public List<ControlElement> getControlElements() {
        return controlElements;
    }

    @Override
    public <A> void accept(NodeVisitor<A> visitor, A arg) {
        visitor.visit(this, arg);
    }

}
