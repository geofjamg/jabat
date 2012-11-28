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
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class Flow extends AbstractNodeContainer implements NodeContainer, Chainable {

    private final String next;

    public Flow(String id, NodeContainer container, String next) {
        super(id, new Properties(), container);
        this.next = next;
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
