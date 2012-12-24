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
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 *
 * @author Geoffroy Jamgotchian <geoffroy.jamgotchian at gmail.com>
 */
public class Job extends AbstractNodeContainer {

    private final List<Artifact> listeners;

    private final boolean restartable;

    Job(String id, Properties properties, Collection<AbstractNode> nodes,
            List<Artifact> listeners, boolean restartable) {
        super(id, properties, nodes);
        this.listeners = Collections.unmodifiableList(listeners);
        this.restartable = restartable;
    }

    public List<Artifact> getListeners() {
        return listeners;
    }

    @Override
    public Artifact getArtifact(String ref) {
        for (Artifact listener : listeners) {
            if (listener.getRef().equals(ref)) {
                return listener;
            }
        }
        throw new JabatException("Artifact " + ref + " not found");
    }

    @Override
    public List<Artifact> getArtifacts() {
        return listeners;
    }

    public boolean isRestartable() {
        return restartable;
    }

    @Override
    public <A> void accept(NodeVisitor<A> visitor, A arg) {
        visitor.visit(this, arg);
    }

}
